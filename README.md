# Pandas vs. PySpark: Performance Analysis
All necessary test files for my Pandas and PySpark comparison. This code is the foundation for my blog article "Pandas vs. PySpark: Performance Analysis" on medium.

Find the article here: [Pandas vs. PySpark: Performance Analysis](https://medium.com/)


## Table of Contents

- [Project Structure](#project-structure)
- [Environment](#environment)
- [Data](#data)
- [Constraints](#constraints)


## Project Structure

```
|-- data
|    |-- large
|    |-- small
|    |-- data/time_statistics_large.csv
|    |-- data/time_statistics_small.csv
|
|-- test
|
|-- 0_data_creation.ipynb
|-- 1_performance_testing.ipynb
|-- 2_data_plotting.ipynb
|-- LICENSE
|-- README.md
|-- test_functions.py
|-- utils.py
```

All the utilized notebooks, functions, and created Python files are meticulously documented within the respective files themselves, providing comprehensive explanations.


## Environment

### Python Version & Libraries

- Python 3.7.3
- ipykernel                     6.15.0
- ipython                       7.33.0
- jupyter-client                7.0.6 
- jupyter_core                  4.11.1
- nbformat                      5.8.0
- pandas                        1.3.5
- pip                           22.3.1
- plotly                        5.15.0
- py4j                          0.10.9.3
- pyspark                       3.2.1


### My Workstation

- MacBook Pro (16-inch, 2019)
- processor: 2,4 GHz 8-Core Intel Core i9
- graphics: Intel UHD Graphics 630 1536 MB
- memory: 32 GB 2667 MHz DDR4
- macOS 13.4.1 




## Data
All the data is created artificial. The code for the creation can be found in the `0_data_creation.ipynb`.
I did not upload the test data, if you want to run the notebooks yourself please create the data first.


## Constraints
Although my machine is quite powerful, it sometimes took a long time to run the code. For this reason I have worked in smaller iterations from time to time. Some tests resulted in a memory overload, others did not run due to the size of the data set. 
I wanted to keep this analysis as simple as possible, therefore I did not use any cloud services or distributed systems. I know this would have demonstrated the power of PySpark even more. But I think the results are still very interesting and show the differences between Pandas and PySpark very well (at least on a single system).
