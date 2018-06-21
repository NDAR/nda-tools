# pip install git+https://github.com/NDAR/nda_aws_token_generator.git#egg=nda-aws-token-generator&subdirectory=python
# cd ~/nda_aws_token_generator/python/
# sudo python setup.py install

from __future__ import with_statement
from __future__ import absolute_import
import sys

if sys.version_info[0] < 3:
	import Queue as queue
	input = raw_input
else:
	import queue
import os
from getpass import getpass
import csv
import threading
import multiprocessing
import boto3
import botocore
import datetime
import xml.etree.ElementTree as ET
#from NDATools.Configuration import *
from NDATools.NDATools import Configuration

import requests


class Download:

	def __init__(self, directory, username=None, password=None):
		self.config = Configuration.ClientConfiguration()
		self.url = self.config.datamanager_api

		if username:
			self.username = username
		else:
			self.username = input('Enter your NIMH Data Archives username:')
		if password:
			self.password = password
		else:
			self.password = getpass.getpass('Enter your NIMH Data Archives password:')

		self.directory = directory
		self.download_queue = queue.Queue()
		self.path_list = set()

	def get_links(self, links, files, filters=None):

		if links == 'datastructure':
			with open(files[0]) as tsv_file:
				tsv = csv.reader(tsv_file, delimiter="\t")
				header = next(tsv)

				if filters:
					filter = filters[0]
					filter = filter.split(',')
					column = filter[0]
					value = filter[1]
					column_index = header.index(column)
					image_file = header.index('image_file')
					for row in tsv:
						if row[column_index] == value:
							self.path_list.add(row[image_file])
				else:
					image_file = header.index('image_file')
					for row in tsv:
						self.path_list.add(row[image_file])
		elif links == 'text':
			with open(files[0]) as tsv_file:
				tsv = csv.reader(tsv_file, delimiter="\t")
				for row in tsv:
					self.path_list.add(row[0])

		elif links == 'package':
			package = files[0]
			# do something with DataManager
			payload = ('<?xml version="1.0" ?>\n' +
			           '<S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">\n' +
			           '<S:Body> <ns3:QueryPackageFileElement\n' +
			           'xmlns:ns4="http://dataManagerService"\n' +
			           'xmlns:ns3="http://gov/nih/ndar/ws/datamanager/server/bean/jaxb"\n' +
			           'xmlns:ns2="http://dataManager/transfer/model">\n' +
			           '<packageId>' + package + '</packageId>\n' +
			           '<associated>true</associated>\n' +
			           '</ns3:QueryPackageFileElement>\n' +
			           '</S:Body>\n' +
			           '</S:Envelope>')


			package_files = api_request(self, "POST", self.url, data=payload)

			for file in package_files:
				#print(file) How to determine which are data structures from which to download data??
				if file.startswith('s3://gpop/ndar_data/QueryPackages/PRODDB/Package_{}/'.format(package)):
					self.path_list.add(file)
		else:
			self.path_list = files

	def check_time(self):
		now_time = datetime.datetime.now()
		if now_time >= self.refresh_time:
			self.get_tokens()

	def get_tokens(self):
		start_time = datetime.datetime.now()
		generator = NDATokenGenerator(self.url)
		self.token = generator.generate_token(self.username, self.password)
		self.refresh_time = start_time + datetime.timedelta(hours=23, minutes=55)


	def queuing(self, resume, prev_directory):
		self.resume = resume
		self.prev_directory = prev_directory
		cpu_num = multiprocessing.cpu_count()
		if cpu_num > 1:
			cpu_num -= 1
		for x in range(cpu_num):
			worker = Download.DownloadTask(self)
			worker.daemon = True
			worker.start()
		for path in self.path_list:
			self.download_queue.put(path)
		self.download_queue.join()

	class DownloadTask(threading.Thread):
		def __init__(self, Download):
			threading.Thread.__init__(self)
			self.download = Download
			self.resume = Download.resume
			self.download_queue = Download.download_queue
			self.url = Download.url
			self.username = Download.username
			self.password = Download.password
			self.directory = Download.directory
			self.access_key = Download.token.access_key
			self.secret_key = Download.token.secret_key
			self.session = Download.token.session
			self.prev_directory = Download.prev_directory

		def resume_file_download(self, file):
			# not working. both file sizes are the same...so is it even a partial download?

			self.download.check_time()
			session = boto3.session.Session(self.access_key,
			                                self.secret_key,
			                                self.session)
			s3client = session.client('s3')

			response = s3client.head_object(Bucket=self.bucket, Key=self.key)
			S3size = response['ContentLength']
			print('S3 file size:', S3size)
			local_size = os.path.getsize(file)
			print('local file size:', local_size)

		def run(self):
			while True:
				path = self.download_queue.get()

				filename = path.split('/')
				self.filename = filename[3:]
				self.key = '/'.join(self.filename)
				self.bucket = filename[2]
				self.newdir = filename[3:-1]
				self.newdir = '/'.join(self.newdir)
				self.newdir = os.path.join(self.directory, self.newdir)
				self.local_filename = os.path.join(self.directory, self.key)

				downloaded = False

				# check previous downloads
				if self.resume:
					prev_local_filename = os.path.join(self.prev_directory, self.key)
					if os.path.isfile(prev_local_filename):
						print(prev_local_filename, 'is already downloaded.')
						downloaded = True

				if not downloaded:
					if not os.path.exists(self.newdir):
						os.makedirs(self.newdir)

					# check tokens
					self.download.check_time()

					session = boto3.session.Session(self.access_key,
					                                self.secret_key,
					                                self.session)
					s3client = session.client('s3')

					try:
						s3client.download_file(self.bucket, self.key, self.local_filename)
						print('downloaded: ', path)
					except botocore.exceptions.ClientError as e:
						# If a client error is thrown, then check that it was a 404 error.
						# If it was a 404 error, then the bucket does not exist.
						error_code = int(e.response['Error']['Code'])
						if error_code == 404:
							print('This path is incorrect:', path, 'Please try again.\n')
							pass
						if error_code == 403:
							print('This is a private bucket. Please contact NDAR for help.\n')
							pass
				self.download_queue.task_done()