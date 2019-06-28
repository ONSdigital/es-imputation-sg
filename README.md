# es-imputation-sg

## Wranglers

### Calculate Movements Wrangler

This is the first step in the imputation section of the process. The Wrangler is responsible for checking for strata miss matches, if an anomaly is detected a notification will be created and handed to the BPM.

As imputation will not always run, there needs to be some way of checking if it's an imputation run or not, this is known as check for non responsers. This task has been dealt with inside of the wranger and will bypass the imputation processing and pass the data on if there are no non-responders.

As the correct practice is to seperate out the creation of columns from the method, this wrangler is responsible for creating each questions responding movement column.

Like every wrangler, it is responsible for dealing with sending data to the SQS Queue so that it can move to the next process, it is also responsible for sending data to the BPM.

### Calculate IQRS Wrangler

This is the third step of the imputation process. The wrangler returns means data from the sqs queue (the output from the calculate means step). It converts this data from JSON format into a dataframe and then adds 7 new IQRS columns (for the 7 questions) onto the dataframe. These 7 columns are initially populated with 0 values within the wrangler.

Next, the wrangler calls the method (see below) which populates the IQRS columns and passes the data back to the wrangler. The wrangler passes the data, in JSON format, back to the SQS queue so it can be used by the next process. It also sends data to the BPM via the SNS queue

### Calculate Atypical Values

This is the fourth step of the imputation process. The wrangler returns iqrs data from the sqs queue (the output from the calculate iqrs step). It converts this data from JSON format into a dataframe and then adds 7 new ATypical columns (for the 7 questions) onto the dataframe. These 7 columns are initially populated with 0 values within the wrangler.

Next, the wrangler calls the method (see below) which populates the Atypical columns and passes the data back to the wrangler. The wrangler passes the data, in JSON format, back to the SQS queue so it can be used by the next process. It also sends data to the BPM via the SNS queue


### Apply Factors Wrangler

This is the final step in the imputation process. This wrangler retrieves factors data from the sqs queue(output from calculate factors) non_responder data from s3(stored in the calculate movements step). Next it retrieves previous period data for the non_responders and joins it on, adding prev_ [question]'s  to each row.

The factors data is merged on to the non responder data next, adding imputation_factor_ [question]'s to each row. At this point the data is prepared in order to send to the method. The merged data is sent to the Apply Factors Method and the result is returned.

The result of the method is imputed values for each non responder, this is joined back onto the responder data(used to calculate factors) and sent to the SQS queue.

## Methods

### Calculate Movements Method

**Name of Lambda:** imputation_calculate_movement _This Method will be re-named to be inkeeping with the standards soon_

**Intro:** The calculate movement method takes the current year's question value, for each question and subtracts the corresponding previous years question value and then divides the result by the current year's question value **e.g. Question_Movement = (Q106_Current_Year - Q106_Previous_Year) / Q106_Current_Year**

**Inputs:** This method will require all of the Questions columns to be on the data which is being sent to the method, e.g. **Q601,Q602...**. A movement_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A Json string which contains all the created movements, saved in the respective movement_*question_name* columns.


### Calculate IQRS method

**Name of Lambda:** iqrs_method  - This Method will be re-named to be inkeeping with the standards soon_

**Intro:** The calculate movement method takes the current year's question value, for each question and subtracts the corresponding previous years question value and then divides the result by the current year's question value **e.g. Question_Movement = (Q106_Current_Year - Q106_Previous_Year) / Q106_Current_Year**

**Inputs:** This method will require all of the Movement columns to be on the data which is being sent to the method, e.g. **Movement_Q601_Asphalting_Sand, Movement_Q602_Building_Soft_Sand,....**. There is also a requirement that the Mean columns should be on the data. It's not used for the IQRS calculation, but it should be passed through for use by later steps.
An iqrs_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A Json string which contains all the created iqrs values, saved in the respective iqrs_*question_name* columns.

## Calculate ATypicals Method

**Name of Lambda:** atypicals_method  - This Method will be re-named to be inkeeping with the standards soon_

**Intro:** The calculate atypical method calculates the atypical value for each row on the dataframe, and for each of the 7 questions, using the following formula (using question 601 as an example)

**abs(Movement_Q601_Asphalting_Sand - Mean601) - 2 * iqrs601

Following this, if the Atypical value is > 0, we recalculate the movement value to be Null. Otherwise, we set the movement column to itself. eg, using Q601 as an example

**If Atyp601 > 0, then Movement_Q601_Asphalting_Sand = null else Movement_Q601_Asphalting_Sand = Movement_Q601_Asphalting_Sand

**Inputs:** This method will require all of the Movement columns, the Mean columns and the IQRS columns to be on the data which is being sent to the method.
An atyp_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A Json string which contains all the created atypical values, saved in the respective atyp_*question_name* columns.

