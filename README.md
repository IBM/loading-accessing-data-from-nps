# Data analytics and prediction using Netezza Performance Server

In this code pattern, we will learn about how users and developers interested in leveraging the development and use of analytic algorithms to perform research or other business-related activities using Netezza Performance Server. Netezza a.k.a. Netezza or INZA, enables data mining tasks on large data sets using the computational power and parallelization mechanisms provided by
the Netezza appliance. The parallel architecture of the Netezza database environment enables high-performance computation on large data sets, making it the ideal platform for largescale data mining applications.

Netezza has in-database Analytics packages for mining the spectrum of data set sizes. IBM Netezza In-Database Analytics is a data mining application that includes many of the key techniques and popular real-world algorithms used with data sets.

In this code pattern, we will be using energy price and Australian weather station temperature and ranfall dataset and analyze the data using Jupyter Notebook using IBM Cloud Pak for Data (CP4D) platform. We will walk you through step by step on:

1. Connecting to Netezza database
1. Loading data to Netezza using csv file, external table, cloud object storage.
1. Analyzing data using Netezza In-Database analytic functions.
1. Creating machine learning models using Netezza In-Database machine learning algorithms.

# Flow

TBD


## Included components

* [Netezza Performance Server](https://www.ibm.com/products/netezza): IBM Netezza® Performance Server for IBM Cloud Pak® for Data is an advanced data warehouse and analytics platform available both on premises and on cloud.
* [IBM Cloud Pak for Data Platform](https://www.ibm.com/products/cloud-pak-for-data) : IBM Cloud Pak® for Data is a fully-integrated data and AI platform that modernizes how businesses collect, organize and analyze data to infuse AI throughout their organizations.
* [Jupyter Notebook](https://jupyter.org/): An open-source web application that allows you to create and share documents that contain live code, equations, visualizations and narrative text.



# Steps

This pattern consists of multiple notebooks that runs in  to connect and load data using multiple sources, Analyzing and creating model using In-Database functions

## Create connection and load data to Netezza
In this section, we will load Jupyter notebook using IBM Cloud Pak for Data platform. The notebook has steps to connect to Netezza and load data from various sources and use Netezza SQL to query data from the netezza database.

Click [here](README-create-load.md) to explore on creating connection and loading data to Netezza Performance Server.

## In-Database analytics using Netezza

In this section, we will load Jupyter notebook using IBM Cloud Pak for Data platfrom. The notebook has steps to connect to Netezza and read data from teh database and apply In-Database analtyics function to analyze the data. We will aslo be creating machine learning models to predict or forecast data.

Click [here](README-analytics.md) to explore on In-Database analtyics using Netezza.

# License

This code pattern is licensed under the Apache Software License, Version 2.  Separate third party code objects invoked within this code pattern are licensed by their respective providers pursuant to their own separate licenses. Contributions are subject to the [Developer Certificate of Origin, Version 1.1 (DCO)](https://developercertificate.org/) and the [Apache Software License, Version 2](https://www.apache.org/licenses/LICENSE-2.0.txt).

[Apache Software License (ASL) FAQ](https://www.apache.org/foundation/license-faq.html#WhatDoesItMEAN)