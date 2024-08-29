import os
import logging
import uuid
import datetime
from importlib import import_module
from deriva.core import get_credential, urlsplit, urlunsplit, format_exception, stob
from deriva.core.utils.hash_utils import compute_hashes
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
        if self.envars:
            target_url = target_url.format(**self.envars)
        target_url = target_url.strip(" ")
        upr = urlsplit(target_url, "https")
        self.scheme = upr.scheme.lower()
        self.netloc = upr.netloc
        self.path = upr.path.strip("/")
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
                                   (role_arn, format_exception(e)))

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

        identity = os.path.basename(self.identity.get("id"))
        object_qualifier = compute_hashes(
            identity.encode() if identity else
            "anon-" + self.envars.get("request_ip", "unknown").encode(), hashes=['md5'])['md5'][0]
        if not stob(self.parameters.get("overwrite", False)):
            now = datetime.datetime.now()
            object_qualifier = "/".join([object_qualifier, now.strftime("%Y-%m-%d_%H.%M.%S")])

        for k, v in self.outputs.items():
            object_name = "/".join([self.path, object_qualifier, k]).lstrip("/")
            file_path = v[LOCAL_PATH_KEY]
            acl = self.parameters.get("acl", "private")
            signed_url = stob(self.parameters.get("signed_url", acl == "public-read"))
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

            if self.callback:
                if not self.callback(progress="Uploaded file [%s] to: %s" % (file_path, remote_path)):
                    break

        return self.outputs
