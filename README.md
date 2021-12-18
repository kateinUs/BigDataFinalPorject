# BigData - Final Project

CS-GY 6513 Big Data Group 14

Group member: Sai, Shourie / Zhang, Huimin / Zhang, Xinyu

## Relate documents

The relate documents and simple descriptions are as follows:

1. data_reference : the reference data we commit to https://github.com/VIDA-NYU/reference-data-repository

- borough_zipcode.csv
- nyu-2451-34509-geojson.json

2. Refined_part1.ipynb: the data cleaning file after improvement, please open by **jupyter notebook**.
3. main.py: the data cleaning file which applies to other similar datasets.
4. search_data.py: a file to collect all column names of NYC datasets on Peel with pyspark.
5. Jaccard.ipynb : a file using Jaccard containment to find similar datasets, please open by **jupyter notebook**
6. Data cleaning at Scale - by ITA Group.pdf : the report.

## Environment Preparation
Install the following libraries from PyPI using `pip` with:

```
pip install openclean 0.2.1
pip install openclena_geo 0.1.0
pip install humanfriendly 10.0
pip install pandas 1.3.4
pip install turfpy 0.0.7
pip install geojson 2.5.0
```

## Running Procedure


##### 1. Perform data profiling and data cleaning on our original dataset: Motor Vehicle Collisions - Crashes
 To run jupyter notebook **Refined_part1.ipynb**:
 We recommend using a virtual environment. Below are two examples for setting up a virtual environment
```
# -- Create a new virtual environment
virtualenv venv
# -- Activate the virtual environment
source venv/bin/activate
```
If you are using the Python distribution from Anaconda, you can setup an environment like this:
```
# -- Create a new virtual environment
conda create -n openclean pip
# -- Activate the virtual environment
conda activate openclean
```
##### 2. Find similar datasets
To run **search_data.py**:

1) Log into NYU's Peel by the command:

```
ssh < NetID >@peel.hpc.nyu.edu
```

2) Put into **csvname.txt** the paths of all of the NYC open datasets on Peel HDFS by command below. Then, we will get a **csvname.txt**. 

```
hfs -ls -C /user/CS-GY-6513/project_data/ > csvname.txt
```

3) Type following commands to enter pyspark environment:

```
module purge
module load python/gcc/3.7.9
pyspark --deploy-mode client
```

4) Copy code of  **search_data.py** into pyspark line by line. Remember to update NetID in the line: *lines = sc.textFile("/user/NetID/csvname.txt")*. When you copy *while* loop, please copy the whole loop together and after copy that loop, tap the "Enter" twice on the keyboard. Then, please wait for a few minutes since it is running until ">>" appears.

5) Then tap "Ctrl+Z" to  exit the environment. 

6) Type the command *ls* to see that we get a findcol.txt .

To run **Jaccard.ipynb**:

Download **findcol.txt** from the Peel HDFS and put it into the directory which contains  **Jaccard.ipynb** and run **Jaccard.ipynb** .


#####  3. Scale the data cleaning method we proposed to other similar datasets

1) Login into Peel, then use scp to transfer the code to Peel

2) Get all the similar datasets from this HDFS directory into your example directory by

```
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.p6bh-gqsg.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.ipu4-2q9a.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.gjm4-k24g.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.fp78-wt5b.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.dpm2-m9mq.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.bty7-2jhb.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.43nn-pn8j.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.3ub5-4ph8.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.2pgc-gcaa.csv
hfs -get /user/CS-GY-6513/project_data/data-cityofnewyork-us.5ziv-wcy4.csv
```

3) Run **main.py** by

```
module load openclean/0.2.1
pip install pandas
pip install geojson
python main.py
```
