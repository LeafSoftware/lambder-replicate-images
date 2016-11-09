import boto3
import logging
import pprint
import os
import os.path
import json
from datetime import datetime

class Replicator:

  REPLICATE_TAG = "LambderReplicate"
  BACKUP_TAG    = "LambderBackup"

  def __init__(self):
    logging.basicConfig()
    self.logger = logging.getLogger()

    # set location of config file
    script_dir = os.path.dirname(__file__)
    config_file = script_dir + '/config.json'

    # if there is a config file in place, load it in. if not, bail.
    if not os.path.isfile(config_file):
      self.logger.error(config_file + " does not exist")
      exit(1)
    else:
      config_data=open(config_file).read()
      config_json = json.loads(config_data)
      self.AWS_SOURCE_REGION=config_json['AWS_SOURCE_REGION']
      self.AWS_DEST_REGION=config_json['AWS_DEST_REGION']

    self.ec2_source = boto3.resource('ec2', region_name=self.AWS_SOURCE_REGION)
    self.ec2_dest = boto3.resource('ec2', region_name=self.AWS_DEST_REGION)

  def get_source_images(self):
    filters = [
      {'Name':'tag-key', 'Values': [self.REPLICATE_TAG]},
      {'Name': 'state', 'Values': ['available']}]
    images = self.ec2_source.images.filter(Filters=filters)
    return images

  # TODO: verify that description is what we want to use here
  def get_dest_images(self, image_id, backupname):
    filters = [{'Name':'description', 'Values': [self.AWS_SOURCE_REGION+'_'+image_id+'_'+backupname]}]
    images = self.ec2_dest.images.filter(Filters=filters)
    return images

  # Takes an image, returns the backup source
  def get_backup_source(self, resource):
    tags = filter(lambda x: x['Key'] == self.BACKUP_TAG, resource.tags)

    if len(tags) < 1:
      return None

    return tags[0]['Value']

  def copy_image(self, image):
    source_image_id = image.image_id
    sourcebackupname = self.get_backup_source(image)
    self.logger.info("Looking for existing replicas of image {0}".format(source_image_id))
    dest_images = self.get_dest_images(source_image_id, sourcebackupname)
    dest_image_count = len(list(dest_images))
    if dest_image_count != 0:
      self.logger.info("Replica found, no need to copy image")
    else:
      self.logger.info("No replica found, copying image {0}".format(source_image_id))
      source_image = self.ec2_dest.Image(source_image_id)
      dest_image_description = self.AWS_SOURCE_REGION+'_'+source_image_id+'_'+sourcebackupname
      copy_output = self.ec2_dest.meta.client.copy_image(
        DryRun=False,
        SourceRegion=self.AWS_SOURCE_REGION,
        SourceImageId=source_image_id,
        Name=image.name,
        Description=dest_image_description
      )
      dest_image_id = copy_output['ImageId']
      dest_image = self.ec2_dest.Image(dest_image_id)

      # wait for image to exist so we can tag it
      waiter = self.ec2_dest.meta.client.get_waiter('image_exists')
      waiter.wait(ImageIds=[dest_image_id])

      dest_image.create_tags(Tags=[
        {'Key': self.REPLICATE_TAG, 'Value': dest_image_description},
        {'Key': self.BACKUP_TAG, 'Value': sourcebackupname}])

  def copy_images(self,images):
    for image in images:
      self.copy_image(image)

  def run(self):

    # replicate any images that need to be replicated
    source_images = self.get_source_images()
    source_image_count = len(list(source_images))

    self.logger.info("Found {0} source images".format(source_image_count))

    self.copy_images(source_images)
