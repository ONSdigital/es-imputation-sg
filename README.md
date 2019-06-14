# es-imputation-sg

## Wranglers

### Calculate Movements Wrangler

This is the first step in the imputation section of the process. The Wrangler is responsible for checking for strata miss matches, if an anomaly is detected a notification will be created and handed to the BPM.

As imputation will not always run, there needs to be some way of checking if it's an imputation run or not, this is known as check for non responsers. This task has been dealt with inside of the wranger and will bypass the imputation processing and pass the data on if there are no non-responders.

As the correct practice is to seperate out the creation of columns from the method, this wrangler is responsible for creating each questions responding movement column.

Like every wrangler, it is responsible for dealing with sending data to the SQS Queue so that it can move to the next process, it is also responsible for sending data to the BPM.

## Methods

### Calculate Movements Method

**Name of Lambda:** imputation_calculate_movement _This Method will be re-named to be inkeeping with the standards soon_

**Intro:** The calculate movement method takes the current year's question value, for each question and subtracts the corresponding previous years question value and then divides the result by the current year's question value **e.g. Question_Movement = (Q106_Current_Year - Q106_Previous_Year) / Q106_Current_Year**

**Inputs:** This method will require all of the Questions columns to be on the data which is being sent to the method, e.g. **Q601,Q602...**. A movement_*question* column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.  

**Outputs:** A Json string which contains all the created movements, saved in the respective movement_*question_name* columns.
