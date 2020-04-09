
Stock Predictor

This codebase originally included code from huseinzol05's project 'Stock-Prediction-Models'. 

Although that project had many useful examples, I decided to try something different - CNN based model prediction.

This required building huge pipelines of data processing and image manipulation. The result you can see here.

I have deleted most of the original codebase. The code that remains should shame no one but me.
 
## Building in Conda

    Command Line:
    
        activate base
        pip install -r requirements.txt
        
        Note: if you cannot install a dep becuase of an existing distutils dep, use '--ignore-installed' flag.

## Running A Test from the Command Line

    python -m pytest -s ./tests/utils/TestDateUtils.py::TestDateUtils::test_get_random_date
    
## Running PySpark
    
    To Set Up Spark:
        
        http://deelesh.github.io/pyspark-windows.html
        
        Note: Add %HADOOP%/bin to your PATH. This step is missing from the above instructions.

    At the command line:
    
        activate base
        
        %SPARK_HOME%/bin/pyspark
        
## Conda Issues

    Conda is giving a Java error: "java gateway process exited before sending its port number"
    
        Attempting to run pip install pyspark
        
            Will upgrade pyspark to 2.4.3-py_0, py4j to 0.10.7-py36_0
            
## Jobs:

a.cmd - Create VirtualEnv
   
b.cmd - Create backup

mls.cmd - Make learning dataset

daily_merge_data_and_split.cmd - Downloads and splits Equity EOD data by symbol.

phd.cmd - Test holdout images for one day against one model

scp.cmd - Collect predictions for date range for one model

sedft.cmd - Get merged EOD data and split files into symbols.

sefs.cmd - Split pre-downloaded equity fundamentals files into individual symbol files.

pac.cmd - Predict and Calculate using a folder of images and model ID. 

# Calculating Test Holdout

    First run phd on every day.
    
    Then collect folders and run scp.
    
    Then run 'test_fast_calc' using score csv
    
    
# Install Dependencies

    At the command line execute: id.cmd
            
## TODO:

1. get_todays_merged_shar_data should be modified to get the most recent trading day's merged data. So it would only update if a nonzero
length file was found in yesterday's records.
Status: Done

2. Get CUDA working.
Status: Done

3. Get calc for 10 trades in a day working.
Status: Done

4. AutoML with PE
Status: Done

4. Get lstm-bahdanau working
Status:

5. Start calc for realtime agent with 100 stocks.
Status:

6. Good performance on test_predict_and_calculate (mixing model with diff rendered chart)

7. Note better performance was gleaned with Rosebud when, at PAC time, volatility_min was set to ~2.0.
        short_model_id = "ICN5283794452616644197" # volreq_fil_09_22_v20190923042914 7/17-8/30: 0.2637%
