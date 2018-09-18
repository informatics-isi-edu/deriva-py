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
        host = urlunsplit((self.scheme, upr.netloc, "", "", ""))
        creds = get_credential(host)
        if not creds:
            logging.info("Unable to locate credential entry for: %s" % host)
        self.credentials = creds or dict()

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
        token = self.credentials.get("token")
        role_arn = self.parameters.get("role_arn")
        profile_name = self.parameters.get("profile")
        region_name = self.parameters.get("region")
        try:
            session = self.BOTO3.session.Session(profile_name=profile_name, region_name=region_name)
        except Exception as e:
            raise DerivaDownloadConfigurationError("Unable to create Boto3 session: %s" % format_exception(e))

        if role_arn:
            try:
                sts = session.client('sts')
                response = sts.assume_role(RoleArn=role_arn, RoleSessionName='DERIVA-Export', DurationSeconds=3600)
                temp_credentials = response['Credentials']
                key = temp_credentials['AccessKeyId']
                secret = temp_credentials['SecretAccessKey']
                token = temp_credentials['SessionToken']
            except Exception as e:
                raise RuntimeError("Unable to get temporary credentials using arn [%s]. %s" %
                                   (role_arn, get_typed_exception(e)))

        try:
            if self.scheme == "gs":
                endpoint_url = "https://storage.googleapis.com"
                config = self.BOTO3.session.Config(signature_version="s3v4")
                kwargs = {"aws_access_key_id": key,
                          "aws_secret_access_key": secret,
                          "endpoint_url": endpoint_url,
                          "config": config}
            else:
                kwargs = {"aws_access_key_id": key, "aws_secret_access_key": secret}
                if token:
                    kwargs.update({"aws_session_token": token})

            s3_client = session.client("s3", **kwargs)
            kwargs["config"] = self.BOTO3.session.Config(signature_version=self.BOTOCORE.UNSIGNED)
            s3_client_unsigned = self.BOTO3.client('s3', **kwargs)
        except Exception as e:
            raise DerivaDownloadError("Unable to create Boto3 storage client: %s" % format_exception(e))

        bucket_name = self.netloc
        bucket_exists = True
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except self.BOTOCORE.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                bucket_exists = False
        except Exception as e:
            raise DerivaDownloadError("Unable to query target bucket: %s" % format_exception(e))

        if not bucket_exists:
            raise DerivaDownloadError("Target bucket [%s] does not exist." % bucket_name)

        for k, v in self.outputs.items():
            object_name = self.path + k
            file_path = v[LOCAL_PATH_KEY]
            acl = self.parameters.get("acl", "private")
            signed_url = strtobool(self.parameters.get("signed_url", str(acl == "public-read")))
            if signed_url:
                client = s3_client_unsigned if acl == "public-read" else s3_client
                remote_path = client.generate_presigned_url(
                    'get_object', Params={'Bucket': bucket_name, 'Key': object_name})
            else:
                remote_path = urlunsplit((self.scheme, self.netloc, object_name, "", ""))
            logging.info("Uploading file [%s] to: %s" % (file_path, remote_path))
            remote_paths = v.get(REMOTE_PATHS_KEY, list())
            remote_paths.append(remote_path)
            v[REMOTE_PATHS_KEY] = remote_paths
            self.make_file_output_values(file_path, v)
            with open(file_path, "rb") as input_file:
                try:
                    response = s3_client.put_object(ACL=acl,
                                                    Bucket=bucket_name,
                                                    Key=object_name,
                                                    Body=input_file,
                                                    ContentType=v[CONTENT_TYPE_KEY],
                                                    ContentLength=v[FILE_SIZE_KEY],
                                                    ContentMD5=v[MD5_KEY][1],
                                                    Metadata={"Content-MD5": v[MD5_KEY][0]})
                except Exception as e:
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
        region = None
        if self.scheme == "s3":
            region = self.parameters.get("region", "us-east-1")
            if region:
                provider = self.LIBCLOUD_DRIVERS["s3"].get(region.lower(), provider)
        if not provider:
            raise DerivaDownloadConfigurationError(
                "%s could not locate a suitable libcloud storage provider driver for URL scheme: %s" %
                (self.__class__.__name__, upr.scheme))

        try:
            cls = self.LIBCLOUD.storage.providers.get_driver(provider)
            driver = cls(region=region, **self.credentials)
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
