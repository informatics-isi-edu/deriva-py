import os
import logging
from importlib import import_module
from deriva.core import get_credential, urlsplit, urlunsplit, format_exception, strtobool
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import *


class UploadPostProcessor(BaseProcessor):
    """
    Post processor that transfers download results to remote systems.
    """

    def __init__(self, envars=None, **kwargs):
        super(UploadPostProcessor, self).__init__(envars, **kwargs)
        self.scheme = None
        self.netloc = None
        self.path = None
        self.credentials = None

    def process(self):
        target_url_param = "target_url"
        target_url = self.parameters.get(target_url_param)
        if not target_url:
            raise DerivaDownloadConfigurationError(
                "%s is missing required parameter '%s' from %s" %
                (self.__class__.__name__, target_url_param, PROCESSOR_PARAMS_KEY))
        upr = urlsplit(target_url, "https")
        self.scheme = upr.scheme.lower()
        self.netloc = upr.netloc
        self.path = upr.path.lstrip("/") + "/" if upr.path else ""
        creds = get_credential(urlunsplit((self.scheme, upr.netloc, "", "", "")))
        if not creds:
            raise DerivaDownloadConfigurationError("Unable to locate credential entry for: %s" % host)
        self.credentials = creds

        return self.outputs


class Boto3UploadPostProcessor(UploadPostProcessor):
    BOTO3 = None
    BOTOCORE = None

    def import_boto3(self):
        # locate library
        if self.BOTO3 is None and self.BOTOCORE is None:
            try:
                self.BOTO3 = import_module("boto3")
                self.BOTOCORE = import_module("botocore")
            except ImportError as e:
                raise DerivaDownloadConfigurationError("Unable to find required module. "
                                                       "Ensure that the Python package \"boto3\" is installed.", e)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            self.BOTO3.set_stream_logger('')

    def __init__(self, envars=None, **kwargs):
        super(Boto3UploadPostProcessor, self).__init__(envars, **kwargs)
        self.import_boto3()

    def process(self):
        super(Boto3UploadPostProcessor, self).process()
        key = self.credentials.get("key")
        secret = self.credentials.get("secret")
        profile_name = self.credentials.get("profile_name")
        region_name = self.credentials.get("region")
        session = self.BOTO3.session.Session(aws_access_key_id=key,
                                             aws_secret_access_key=secret,
                                             profile_name=profile_name,
                                             region_name=region_name)

        if self.scheme == "gs":
            s3 = session.resource("s3", endpoint_url="https://storage.googleapis.com",
                                  config=self.BOTO3.session.Config(signature_version="s3v4"))
        else:
            s3 = session.resource("s3")

        s3_client_signed = self.BOTO3.client('s3')
        s3_client_unsigned = self.BOTO3.client(
            's3', config=self.BOTO3.session.Config(signature_version=self.BOTOCORE.UNSIGNED))

        bucket_name = self.netloc
        bucket_exists = True
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except self.BOTOCORE.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                bucket_exists = False

        if not bucket_exists:
            raise DerivaDownloadError("Target bucket [%s] does not exist." % bucket_name)

        for k, v in self.outputs.items():
            object_name = self.path + k
            file_path = v[LOCAL_PATH_KEY]
            acl = self.parameters.get("acl", "private")
            signed_url = strtobool(self.parameters.get("signed_url", str(acl == "public-read")))
            if signed_url:
                client = s3_client_unsigned if acl == "public-read" else s3_client_signed
                remote_path = client.generate_presigned_url(
                    'get_object', Params={'Bucket': bucket_name, 'Key': object_name})
            else:
                remote_path = urlunsplit((self.scheme, self.netloc, object_name, "", ""))
            remote_paths = v.get(REMOTE_PATHS_KEY, list())
            remote_paths.append(remote_path)
            v[REMOTE_PATHS_KEY] = remote_paths
            self.make_file_output_values(file_path, v)
            with open(file_path, "rb") as input_file:
                s3_object = s3.Object(bucket_name, object_name)
                try:
                    response = s3_object.put(ACL=acl,
                                             Body=input_file,
                                             ContentType=v[CONTENT_TYPE_KEY],
                                             ContentLength=v[FILE_SIZE_KEY],
                                             ContentMD5=v[MD5_KEY][1],
                                             Metadata={"Content-MD5": v[MD5_KEY][0]})
                except self.BOTOCORE.exceptions.ClientError as e:
                    raise DerivaDownloadError("Upload of %s failed: %s" % (remote_path, format_exception(e)))

        return self.outputs


class LibcloudUploadPostProcessor(UploadPostProcessor):
    """
    Post processor that transfers download results to remote cloud storage systems via libcloud library.
    There are various problems with it that make it not viable at this time.
    """
    LIBCLOUD = None
    LIBCLOUD_DRIVERS = None

    def import_libcloud(self):
        # locate library
        if self.LIBCLOUD is None:
            try:
                self.LIBCLOUD = import_module("libcloud")
            except ImportError as e:
                raise DerivaDownloadConfigurationError("Unable to find required module. "
                                                       "Ensure that the Python package \"libcloud\" is installed.", e)
            self.LIBCLOUD_DRIVERS = {
                "s3": {
                    "us-east-1": self.LIBCLOUD.storage.types.Provider.S3,
                    "us-east-2": self.LIBCLOUD.storage.types.Provider.S3_US_EAST2,
                    "us-west-1": self.LIBCLOUD.storage.types.Provider.S3_US_WEST,
                    "us-west-2": self.LIBCLOUD.storage.types.Provider.S3_US_WEST_OREGON,
                    "us-gov-west-1": self.LIBCLOUD.storage.types.Provider.S3_US_GOV_WEST,
                    "cn-north-1": self.LIBCLOUD.storage.types.Provider.S3_CN_NORTH,
                    "eu-west-1": self.LIBCLOUD.storage.types.Provider.S3_EU_WEST,
                    "eu-west-2": self.LIBCLOUD.storage.types.Provider.S3_EU_WEST2,
                    "eu-central-1": self.LIBCLOUD.storage.types.Provider.S3_EU_CENTRAL,
                    "ap-south-1": self.LIBCLOUD.storage.types.Provider.S3_AP_SOUTH,
                    "ap-southeast-1": self.LIBCLOUD.storage.types.Provider.S3_AP_SOUTHEAST,
                    "ap-southeast-2": self.LIBCLOUD.storage.types.Provider.S3_AP_SOUTHEAST2,
                    "ap-northeast": self.LIBCLOUD.storage.types.Provider.S3_AP_NORTHEAST1,
                    "ap-northeast-1": self.LIBCLOUD.storage.types.Provider.S3_AP_NORTHEAST1,
                    "ap-northeast-2": self.LIBCLOUD.storage.types.Provider.S3_AP_NORTHEAST2,
                    "sa-east-1": self.LIBCLOUD.storage.types.Provider.S3_SA_EAST,
                    "ca-central-1": self.LIBCLOUD.storage.types.Provider.S3_CA_CENTRAL
                },
                "gs": self.LIBCLOUD.storage.types.Provider.GOOGLE_STORAGE
            }

    def __init__(self, envars=None, **kwargs):
        super(LibcloudUploadPostProcessor, self).__init__(envars, **kwargs)
        self.import_libcloud()

    def process(self):
        super(LibcloudUploadPostProcessor, self).process()
        provider = self.LIBCLOUD_DRIVERS.get(self.scheme)
        if self.scheme == "s3":
            region = self.credentials.get("region", "us-east-1")
            if region:
                provider = self.LIBCLOUD_DRIVERS["s3"].get(region.lower(), provider)

        if not provider:
            raise DerivaDownloadConfigurationError(
                "%s could not locate a suitable libcloud storage provider driver for URL scheme: %s" %
                (self.__class__.__name__, upr.scheme))

        try:
            cls = self.LIBCLOUD.storage.providers.get_driver(provider)
            driver = cls(**self.credentials)
            container = driver.get_container(container_name=self.netloc)

            for k, v in self.outputs.items():
                object_name = self.path + k
                file_path = v[LOCAL_PATH_KEY]
                remote_path = urlunsplit((self.scheme, self.netloc, object_name, "", ""))
                remote_paths = v.get(REMOTE_PATHS_KEY, list())
                remote_paths.append(remote_path)
                v[REMOTE_PATHS_KEY] = remote_paths
                self.make_file_output_values(file_path, v)
                result = driver.upload_object(file_path,
                                              container,
                                              object_name,
                                              extra={"acl": self.parameters.get("acl")},
                                              verify_hash=True)

        except self.LIBCLOUD.common.types.LibcloudError as lce:
            raise DerivaDownloadError(format_exception(lce))

        return self.outputs
