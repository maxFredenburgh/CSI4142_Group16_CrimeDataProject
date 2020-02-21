#OS - Data Preprocessing
#Author: Nic Gardin

import pandas as pd
import numpy as np
import time


def getCrimeKeysVan(df):

	startTime = time.perf_counter()
	vanc = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crime_datasets/crimeVan.csv", encoding="UTF-8")
	crimeKeys = pd.DataFrame(-1, index=np.arange(len(df)),columns=['crimeKey', 'isNighttime'])

	#1. for Van
	for i in range(len(df)):
		hour = vanc.iloc[i]["HOUR"]
		minute = vanc.iloc[i]["MINUTE"]

		if pd.isnull(hour) or pd.isnull(minute):
			continue

		repTime =  str(int(hour)) + ":" + str(int(minute))
		details = vanc.iloc[i]["TYPE"]

		#get specific crime from crimeDim.csv
		sCrime = df.loc[(df['reportedTime'] == repTime) & (df['crimeDetails'] == details)]

		key = sCrime['crimeKey'].iloc[0]
		crimeKeys.loc[i] = key

		isNight = sCrime['timeOfDay'].iloc[0]
		
		if (isNight==0) or (isNight == 3):
			crimeKeys['isNighttime'].loc[i] = 1
		else:
			crimeKeys['isNighttime'].loc[i] = 0

		if(i%10000 == 0):
			# endTime = time.perf_counter()
			# print(endTime-startTime)
			# print(i)
			print(repTime + "    " + details + "    " + str(key))

	return crimeKeys


def getCrimeKeysDen(df):

	startTimeCounter = time.perf_counter()
	denc = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crime_datasets/crimeDen.csv", encoding="UTF-8")
	denc.dropna(subset=['FIRST_OCCURRENCE_DATE'])

	denc['REPORTED_DATE'] = pd.to_datetime(denc['REPORTED_DATE'], format='%m/%d/%Y %I:%M:%S %p')
	denc['REPORTED_DATE'] = denc['REPORTED_DATE'].astype(str)

	crimeKeys = pd.DataFrame(-1, index=np.arange(len(df)),columns=['crimeKey', 'isNighttime'])

	df.dropna(subset=['crimeStartTime'])

	for i in range(len(df)):

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

			key = sCrime['crimeKey'].iloc[0]
			crimeKeys.loc[i] = key

			isNight = sCrime['timeOfDay'].iloc[0]
			if pd.isnull(isNight):
				continue

			if (isNight==0) or (isNight == 3):
				crimeKeys['isNighttime'].loc[i] = 1
			else:
				crimeKeys['isNighttime'].loc[i] = 0

			# print("Row " + str(i) + " info:  " + repTime + "    "  + details + "    " + startTime + "\nKey found: "  + str(key) + "\n") # + "\nKey found: "  + str(key) + "\n"


			if(i%10000 == 0):
				endTimeCounter = time.perf_counter()
				print(endTimeCounter-startTimeCounter)
				print("Row " + str(i) + " info:  " + repTime + "    "  + details + "    " + startTime + "\nKey found: "  + str(key) + "\n") # + "\nKey found: "  + str(key) + "\n"
		
		except:
			continue

	return crimeKeys


if __name__ == '__main__':

	#crimeDim
	crime_df = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/CSI4142_Group16_CrimeDataProject/crimeDim.csv", encoding="UTF-8")

	#split crimeDim.csv
	denCrimeDim = crime_df.head(461699)
	vanCrimeDim = crime_df.tail(17352)

	# vanCrimeDim.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/vanSet.csv', index=False)
	# denCrimeDim.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/denSet.csv', index=False)

	# create and write vanCrimeKeys.csv
	vanCrimeKeys = getCrimeKeys(vanCrimeDim)
	vanCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/vanCrimeKeys.csv', index=False)

	# create and write denCrimeKeys.csv (SLOW)
	denCrimeKeys = getCrimeKeysDen(denCrimeDim)
	denCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/denCrimeKeys.csv', index=False)

	# create and write denCrimeKeys.csv (FAST)
	# split into groups of 100,000
	# denCrimeDim1 = denCrimeDim.head(100000)
	# denCrimeDim2 = denCrimeDim1.head(100000)
	# denCrimeDim3 = denCrimeDim2.head(100000)
	# denCrimeDim4 = denCrimeDim3.head(100000)
	# denCrimeDim5 = denCrimeDim4.head(61685)

	# # create 
	# denCrimeKeys1 = getCrimeKeysDen(denCrimeDim1)
	# denCrimeKeys2 = getCrimeKeysDen(denCrimeDim2)
	# denCrimeKeys3 = getCrimeKeysDen(denCrimeDim3)
	# denCrimeKeys4 = getCrimeKeysDen(denCrimeDim4)
	# denCrimeKeys5 = getCrimeKeysDen(denCrimeDim5)

	# # append
	# denCrimeKeysArr = [denCrimeKeys1,denCrimeKeys2,denCrimeKeys3,denCrimeKeys4,denCrimeKeys5]
	# denCrimeKeys = pd.concat(denCrimeKeysArr)

	# write final denCrimeKeys.csv
	# denCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/denCrimeKeys.csv', index=False)\

	# merge Denver and Vancouver datasets
	# vanCrimeKeys = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/vanCrimeKeys.csv")
	# denCrimeKeys = pd.read_csv("/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/denCrimeKeys.csv")
	# denCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/vanCrimeKeys.csv', mode='a', header=False)
	# denCrimeKeys.to_csv('/Users/nicgardin/Documents/2020_Classes_Master/DataScience/crimeKeys/crimeKeys.csv', index=False)

	

