import os
import pandas as pd
import sys
import json
import chardet
from constants import COUNTRY_DICT
import numpy as np
import copy
from collections import defaultdict
file_prefix = "installs_com.cuelearn.cuemathapp_"

class DailyGoogleUpdates:

	def __init__(self,dt):
		self.dt = dt
		self.get_data(dt)
		self.df_overview = None
		self.df_country = None
		self.df_app_version = None

	def is_data_updated(self,dt):
		"""
		Check YYYYMM file for row with date dt. 
		If exists, then return True
		else return False
		"""
		date = dt.split('-')
		year_month = date[0]+date[1]
		directory = "raw_folder/"+"install"+year_month
		if os.path.exists(directory):
			with open(directory+'/'+file_prefix+year_month+'_overview.csv','rb') as A:
				result = chardet.detect(A.read())
			overview = pd.read_csv(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_overview.csv',encoding = result['encoding'])
			df = pd.DataFrame(overview)
			# d = df[df['Date']==date]
			if dt in df[['Date']]:
				return False
		else:
			return True

	def update_data_from_bucket(self,dt):
		"""
		Update saved csv files of current Month.
		"""
		date = dt.split('-')
		year_month = date[0]+date[1]
		directory = "raw_folder/"+"install"+year_month
		print("Making directory")
		path = os.getcwd()
		print(path)
		os.makedirs(directory, exist_ok=True)
		os.system("gsutil cp gs://pubsite_prod_rev_08162167020839892838/stats/installs/" + file_prefix + year_month+"*  ~/Desktop/stats/"+directory)
		# self.get_data(directory,year_month)


	def get_data(self,dt):
		if self.is_data_updated(dt):
			self.update_data_from_bucket(dt)
		date = dt.split('-')
		year_month = date[0]+date[1]
		directory = "raw_folder/"+"install"+year_month
		date = dt.split('-')
		year_month = date[0]+date[1]
		df_overview, df_country, df_app_version = self.read_latest_files_from_folder(directory,year_month)
		self.df_overview = df_overview
		self.df_country = df_country
		self.df_app_version = df_app_version
		# row_data = self.summarize_row()
		# write_row(row_data)

	def read_latest_files_from_folder(self,directory,year_month):
		with open(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_overview.csv','rb') as A:
			result = chardet.detect(A.read())
		overview = pd.read_csv(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_overview.csv',encoding = result['encoding'])
		overview = pd.DataFrame(overview)
		overview.drop(columns = ['Daily Device Uninstalls', 'Daily Device Upgrades','Total User Installs'], axis=1)
		with open(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_country.csv','rb') as B:
				result = chardet.detect(B.read())
		country = pd.read_csv(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_country.csv',encoding = result['encoding'])
		country = pd.DataFrame(country)
		country = country[country['Country'].notna()]
		country = country.dropna()
		with open(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_app_version.csv','rb') as C:
			result = chardet.detect(C.read())
		app_version = pd.read_csv(directory+'/'+'installs_com.cuelearn.cuemathapp_'+year_month+'_app_version.csv',encoding = result['encoding'])
		app_version = pd.DataFrame(app_version)
		app_version = app_version[app_version['App Version Code'].notna()]
		app_version = app_version.dropna()
		return overview,country,app_version

	def transform_to_dict(self,df, dt, column_key, value_key):
		sub_df = df[df['Date'] == dt]
		out_dict = {}
		for idx, row in sub_df.iterrows():
			if(row[value_key] > 0):
				if(column_key=='Country'):
					out_dict[COUNTRY_DICT[row[column_key]]] = row[value_key]
				else:
					out_dict[row[column_key]] = row[value_key]
		return out_dict

	def transform_countries(self):
		installs_countrywise =  self.transform_to_dict(self.df_country, self.dt, 'Country', 'Install events')
		uninstalls_countrywise =  self.transform_to_dict(self.df_country, self.dt, 'Country', 'Uninstall events')
		return installs_countrywise, uninstalls_countrywise

	def transform_appVersion(self):
		installs_appversion =  self.transform_to_dict(self.df_app_version, self.dt, 'App Version Code', 'Install events')
		uninstalls_appversion =  self.transform_to_dict(self.df_app_version, self.dt, 'App Version Code', 'Uninstall events')
		return installs_appversion, uninstalls_appversion

	def summarize_row(self):
		row = self.df_overview[self.df_overview['Date'] == self.dt]
		installs_countrywise, uninstalls_countrywise = self.transform_countries()
		installs_appversion, uninstalls_appversion = self.transform_appVersion()
		meta_installs = {
			"country": installs_countrywise,
			"app_version": installs_appversion
		}
		meta_uninstalls = {
			"country": uninstalls_countrywise,
			"app_version": uninstalls_appversion
		}
		row['meta_installs'] = json.dumps(meta_installs)
		row['meta_uninstalls'] = json.dumps(meta_uninstalls)
		# row = pd.DataFrame(row)
		# row.drop(['Daily Device Uninstalls', 'Daily Device Upgrades','Total User Installs'], axis=1)
		return row
		
def write_row(data):
	data.drop(columns = ['Daily Device Uninstalls', 'Daily Device Upgrades','Total User Installs'], axis=1)
	data.to_csv (r'exported_overview.csv', index = True, header=True)



def main(argv):
	# pull = DailyGoogleUpdates(argv[1])
	start_date,end_date = argv[1],argv[2]
	daterange = pd.date_range(start_date, end_date)
	row = []
	for single_date in daterange:
		dt = single_date.strftime("%Y-%m-%d")
		update_obj = DailyGoogleUpdates(dt)
		update_obj.get_data(dt)
		row.append(update_obj.summarize_row())
	data = pd.concat(row)
	write_row(data)



	# pull.get_data()
if __name__ == '__main__':
	arg = sys.argv[:]
	main(arg)
