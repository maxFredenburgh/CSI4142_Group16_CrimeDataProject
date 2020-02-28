#OS - Crime Keys Creation
#Author: Nic Gardin

import pandas as pd
import numpy as np
import time


def createCrimeDim():

	crimeVan = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/outputCSV/van_crimeDim/van_crimeDim.csv", encoding="UTF-8", names=["crimeCategory", "crimeSeverity", "reportedTime", "timeOfDay", "crimeStartTime", "crimeEndTime", "crimeDetails"])
	crimeDen = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/outputCSV/den_crimeDim/den_crimeDim.csv", encoding="UTF-8")
	crimeVan = crimeVan.drop_duplicates()
	crimeDen = crimeDen.drop_duplicates()
	crimeDim = crimeVan.append(crimeDen)
	crimeDim = crimeDim.reset_index(drop=True)
	crimeDim.index.names =['crimeID']
	return crimeDim


def getCrimeKeysVan(df):

	startTime = time.perf_counter()
	vanc = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crime_datasets/crimeVan.csv", encoding="UTF-8")
	crimeKeys = pd.DataFrame(-1, index=np.arange(len(vanc)),columns=['crimeKey', 'isNighttime'])

	#1. for Van
	for i in range(len(vanc)):
		hour = vanc.iloc[i]["HOUR"]
		minute = vanc.iloc[i]["MINUTE"]

		if pd.isnull(hour) or pd.isnull(minute):
			crimeKeys['crimeKey'].loc[i] = -1
			crimeKeys['isNighttime'].loc[i] = -1
			continue

		repTime =  str(int(hour)) + ":" + str(int(minute))
		details = vanc.iloc[i]["TYPE"]

		#get specific van crime from crimeDim.csv
		sCrime = df.loc[(df['reportedTime'] == repTime) & (df['crimeDetails'] == details)]

		key = sCrime['crimeID'].iloc[0]
		crimeKeys.loc[i] = key

		isNight = sCrime['timeOfDay'].iloc[0]

		if (isNight==0) or (isNight == 3):
			crimeKeys['isNighttime'].loc[i] = 1
		else:
			crimeKeys['isNighttime'].loc[i] = 0

		if(i%10000 == 0):
			# endTime = time.perf_counter()
			print(i)
			print(repTime + "    " + details + "    " + str(key))

	return crimeKeys


def getCrimeKeysDen(df):

	startTimeCounter = time.perf_counter()
	denc = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crime_datasets/crimeDen.csv", encoding="UTF-8")
	denc.dropna(subset=['FIRST_OCCURRENCE_DATE'])

	denc['REPORTED_DATE'] = pd.to_datetime(denc['REPORTED_DATE'], format='%m/%d/%Y %I:%M:%S %p')
	denc['REPORTED_DATE'] = denc['REPORTED_DATE'].astype(str)

	crimeKeys = pd.DataFrame(-1, index=np.arange(len(denc)),columns=['crimeKey', 'isNighttime'])

	df.dropna(subset=['crimeStartTime'])

	for i in range(len(denc)):

		try:
			reportedTime = denc.iloc[i]["REPORTED_DATE"]
			if pd.isnull(reportedTime):
				continue

			startTime = denc.iloc[i]['FIRST_OCCURRENCE_DATE']

			startTime = startTime.split(" ")[0]

			startTimeSpl = startTime.split("/")
			#append leading 0s
			if (len(startTimeSpl[0]) == 1):
				startTime = "0" + startTime

			if (len(startTimeSpl[1]) == 1):
				startTime = startTime[0:3] + "0" + startTime[3:]

			repTime = reportedTime.split(" ")[1]
			repTime = repTime[:-3]
			details = denc.iloc[i]["OFFENSE_TYPE_ID"]

			# print(repTime + "    " + details + "    " + startTime)

			#get specific crime from denCrimeDim.csv
			# if (pd.isnull((df.loc[df['crimeStartTime']]))):
			# 	continue
			sCrime = df.loc[(df['reportedTime'] == repTime) & (df['crimeStartTime'] == startTime) & (df['crimeDetails'] == details)]

			key = sCrime['crimeID'].iloc[0]
			crimeKeys.loc[i] = key

			isNight = sCrime['timeOfDay'].iloc[0]
			if pd.isnull(isNight):
				continue

			if (isNight==0) or (isNight == 3):
				crimeKeys['isNighttime'].loc[i] = 1
			else:
				crimeKeys['isNighttime'].loc[i] = 0

			# print("Row " + str(i) + " info:  " + repTime + "    "  + details + "    " + startTime + "\nKey found: "  + str(key) + "\n") # + "\nKey found: "  + str(key) + "\n"


			if(i%20000 == 0):
				endTimeCounter = time.perf_counter()
				print(endTimeCounter-startTimeCounter)
				print("Row " + str(i) + " info:  " + repTime + "    "  + details + "    " + startTime + "\nKey found: "  + str(key) + "\n") # + "\nKey found: "  + str(key) + "\n"

		except:
			continue

	return crimeKeys


if __name__ == '__main__':

	# create crimeDim
	crimeDim = createCrimeDim()
	crimeDim.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeDim/crimeDim.csv')

	# read crimeDim, select needed cols, split into van and den sets
	crimeDim = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeDim/crimeDim.csv", encoding="UTF-8")
	crimeDim = crimeDim[["crimeID", "reportedTime", "timeOfDay", "crimeStartTime", "crimeDetails"]]
	crimeDim = crimeDim.drop_duplicates()
	vanCrimeDim = crimeDim.head(11456)
	denCrimeDim = crimeDim.tail(461699)

	# create and write vanCrimeKeys.csv
	vanCrimeKeys = getCrimeKeysVan(vanCrimeDim)
	vanCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/newCrimeKeys/vanCrimeKeysTHREE.csv', index=False)

	# create and write denCrimeKeys.csv (SLOW)
	denCrimeKeys = getCrimeKeysDen(denCrimeDim)
	denCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/newCrimeKeys/denCrimeKeys.csv', index=False)

	# append van and den key sets
	crimeKeysDen = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/newCrimeKeys/vanCrimeKeys.csv", encoding="UTF-8")
	crimeKeysVan = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/newCrimeKeys/crimeKeysDENV.csv", encoding="UTF-8")
	crimeKeys = crimeDimVan.append(crimeDimDen)
	crimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/newCrimeKeys/crimeKeys.csv', index=False)
