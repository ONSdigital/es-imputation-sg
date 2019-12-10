# es-imputation-sg

## Wranglers

### Calculate Movements Wrangler

This is the first step in the imputation section of the process. The Wrangler is responsible for checking if the data contains strata, if it does it checks for strata miss matches, if an anomaly is detected a notification will be created and handed to the BPM.

As imputation will not always run, there needs to be some way of checking if it's an imputation run or not, a step known as check for non-responders. This wrangler performs that check if there are no non-responders, it will then bypass the imputation processing and pass the data to the next module.

As the correct practice is to separate out the creation of columns from the method, this wrangler is responsible for creating each question responding movement column.

Like every wrangler, it is responsible for saving data to S3 for the next process. Completion status is published to SNS.

### Calculate Means Wrangler

This is the second step in the imputation process. The wrangler ingests data from the movements step, checks for anomalies, and formats the data to be passed through to the method.

Formatting of the data involves adding blank means columns for each question, for the results of the calculations in the method to be added to.

Like every wrangler, it is responsible for saving data to S3 for the next process. Completion status is published to SNS.

### Calculate Imputation Factors Wangler

This step is a combination of both calculate gb and non-gb factors.

Data is retrieved from the previous step from S3, then the data set is prepared by the addition of the factors columns. This is then passed to the method lambda which calculates the factors.

Once this has been calculated then the data is sent back to the S3 for use by the next method. Completion status is published to SNS.


### Calculate IQRS Wrangler

This is the third step of the imputation process. The wrangler reads the means data from the S3 (the output from the calculate means step). It adds new IQRS columns (one for each question) onto the DataFrame. These columns are initially populated with 0 values within the wrangler.

Next, the wrangler calls the method (see below) which populates the IQRS columns and passes the data back to the wrangler. The wrangler saves the data to S3 so it can be used by the next process. Completion status is published to SNS.

### Calculate Atypical Values Wrangler

This is the fourth step of the imputation process. The wrangler returns IQRS data from S3 (the output from the calculate IQRS step). It converts this data from JSON format into a DataFrame and then adds new Atypical columns (one for each question) onto the DataFrame. These columns are initially populated with 0 values within the wrangler.

Next, the wrangler calls the method (see below) which populates the Atypical columns and passes the data back to the wrangler. The wrangler saves the data to S3 so it can be used by the next process. Completion status is published to SNS.


### Apply Factors Wrangler

This is the final step in the imputation process. This wrangler retrieves factors data from S3 (output from Calculate Factors) non_responder data from s3 (stored in the Calculate Movements step). Next, it retrieves previous period data for the non_responders and joins it on, adding prev_ [question]'s to each row.

The factors data is merged on to the non-responder data next, adding imputation_factor_ [question]'s to each row. The merged data is sent to the Apply Factors Method.

The result of the method is imputed values for each non-responder, this is joined back onto the responder data (used to calculate factors) and saved the data to S3 so it can be used by the next process. Completion status is published to SNS.

### Recalculate Means Wrangler

This wrangler recalculates the means of movement after the atypical values have been removed.

This uses the same method as calculate means.

## Methods

### Calculate Movements Method

**Name of Lambda:** imputation_calculate_movement_method.

**Intro:** The calculate movement method takes the current year's question value, for each question and subtracts the corresponding previous years question value and then divides the result by the current year's question value **e.g. Question_Movement = (Q106_Current_Year - Q106_Previous_Year) / Q106_Current_Year**

**Inputs:** This method will require all of the Questions columns to be on the data which is being sent to the method, **e.g. Q601, Q602...**. A movement_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string which contains all the created movements, saved in the respective movement_*question_name* columns when successful or an error_message when not.


### Calculate Means Method

**Name of Lambda:** imputation_calculate_means_method

**Intro:** The calculate means method takes the movement values of each question, grouped by region and strata, and calculates the mean of each movement value. The result is then stored in a new means column.

**Inputs:** This method will require all of the Questions columns and movement columns to be on the data which is being sent to the method, **e.g. Q601, Q602...**. A means_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string which contains all the created means values, saved in the respective means_*question_name* columns when successful or an error_message when not.


### Calculate Imputation Factors Method

**Name of Lambda:** imputation_calculate_imputation_factors_method.

**Intro:** Calculates imputation factor for each question, in each aggregated group. Factors are calculated depending on the Region, Land or Marine, Count of refs within the cell. 

**Inputs:** JSON string from wrangler with the needed columns.

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string containing imputation factors for each question in each aggregated group when successful or an error_message when not.


### Calculate IQRS method

**Name of Lambda:** iqrs_method

**Intro:** For each distinct Region/Strata group within the dataset, we want to work out the 25th percentile of each movement column - **e.g the 25th percentile of the Movement_Q601_Asphalting_Sand for the group which has a region of 9 and a strata of E.**

 We also want to calculate the 75% percentile of each movement column of the same groups - **e.g. the 7
 5th percentile of the Movement_Q601_Asphalting_Sand for the group which has a region of 9 and a strata of E.** 
 
 The IQRS value for each question is calculated as 75th percentile - 25th percentile.

**Inputs:** This method will require all of the Movement columns to be on the data which is being sent to the method, **e.g. Movement_Q601_Asphalting_Sand, Movement_Q602_Building_Soft_Sand,....**. There is also a requirement that the Mean columns should be on the data. It's not used for the IQRS calculation, but it should be passed through for use by later steps.
An iqrs_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string which contains all the created IQRS values, saved in the respective iqrs_*question_name* columns when successful or an error_message when not.


### Calculate Atypicals Method

**Name of Lambda:** atypicals_method

**Intro:** The Calculate Atypical Method calculates the atypical value for each row on the DataFrame, and for each question, using the following formula (using question 601 as an example):

**abs(Movement_Q601_Asphalting_Sand - Mean601) - 2 * iqrs601**

Following this, if the atypical value is > 0, we recalculate the movement value to be `null`. Otherwise, we set the movement column to itself. eg, using Q601 as an example:

**If Atyp601 > 0, then Movement_Q601_Asphalting_Sand = null else Movement_Q601_Asphalting_Sand = Movement_Q601_Asphalting_Sand**

**Inputs:** This method will require all of the Movement columns, the Mean columns and the IQRS columns to be on the data which is being sent to the method.
An atyp_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string which contains all the created atypical values, saved in the respective atyp_*question_name* columns when successful or an error_message when not.


### Apply Factors Method

**Name of Lambda:** imputation_apply_factors_method 

**Intro:** The apply factors method takes in a DataFrame containing current period data, previous period data, and imputation factors, all on one row. It then performs as a row-by-row apply method the calculation: - current_value = prev_value * imputation_factor for each of the value columns. Finally drops the previous period data and imputation factor from the processed DataFrame

**Inputs:** This method requires all question value columns for the current period, question_value columns for the previous period, and imputation factors for each question value column. Note: Method receives rows that have not responded in the current period but did in the previous.

**Outputs:** A dictionary containing a Success flag (True/False) and a JSON string which represents the input - (prev_question_columns & imputation_factor columns) when successful or an error_message when not. Current question value columns are now imputed.

## Imputation Functions

### Movement Calculation A

**Intro:** Movements calculation for Sand and Gravel.

**Inputs:** Current and Previous values.

**Outputs:** Calculated Value.

### Movement Calculation B

**Intro:** Movements calculation for Bricks/Blocks.

**Inputs:** Current and Previous values.

**Outputs:** Calculated Value.

### Produce Columns

**Intro:** Produces columns with a prefix, suffix and extra columns, based on standard columns.

**Inputs:** Prefix String, List of Columns, Suffix String, List of Extra Columns.

**Outputs:** New List of Columns.

