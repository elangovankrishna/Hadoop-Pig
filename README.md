# Hadoop-Pig
Pig Latin Scripts

Problem Statement
A. Finding the list of people with particular grade who have taken loan.
B. Finding the list of people with having interest more than certain value like 1000.
C. Finding the list of people with having loan amount more than certain value.
D. Get maximum number of loan given to which grade users (A-G).
E. Highest loan amount given in that year with that Employee id and Employees annual income.
F. Get the total number of loans with loan id and load amount which are all having loan status as Late?
G. Average loan interest rate with 60 month term and 36 month term.
Dataset
https://edureka.wistia.com/medias/cpj3ljetym/download?media_file_id=64495520
Dataset Description
https://edureka.wistia.com/medias/410pi4dlfe/download?media_file_id=64495579

-- Loan Project Scripts 

loandata = Load '/user/edureka/learning/loan_project/LoanData_Fin.csv' using PigStorage('\t') as (id:int, member_id:int, loan_amnt:int, funded_amnt:int, funded_amnt_inv:int, term:chararray, int_rate:long, installment:long, grade:chararray, sub_grade:chararray, emp_title:chararray, emp_length:chararray, home_ownership:chararray, annual_inc:int, is_inc_v:chararray, issue_d:chararray, loan_status:chararray, pymnt_plan:chararray, url:chararray, desc:chararray, purpose:chararray, title:chararray, zip_code:chararray, addr_state:chararray, dti:int, delinq_2yrs:int, earliest_cr_line:int, inq_last_6mths:int, mths_since_last_delinq:int, mths_since_last_record:int, open_acc:chararray, pub_rec:int, revol_bal:int, revol_util:long, total_acc:int, initial_list_status:chararray, out_prncp:double, out_prncp_inv:long, total_pymnt:long, total_pymnt_inv:long, total_rec_prncp:long, total_rec_int:long, total_rec_late_fee:long, recoveries:long, collection_recovery_fee:long, last_pymnt_d:Chararray, last_pymnt_amnt:Chararray, next_pymnt_d:Chararray, ast_credit_pull_d:Chararray, collections_12_mths_ex_med:Chararray, mths_since_last_major_derog:Chararray, policy_code:Chararray);

-- Problem Stmts 
-- A: Finding the list of people with particular grade who have taken loan.

grplngrade = Group loandata by grade;

Finres_a = FOREACH grplngrade GENERATE $0, COUNT($1);

Store Finres_a into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_a1.txt';

-----------------------------------------------------------------------------------------------
-- B: Finding the list of people with having interest more than certain value like 1000.
-- C: Finding the list of people with having loan amount more than certain value.
-- Both stmt logic will be similar so i have combined both them into one.

fillndata = filter loandata by loan_amnt >= 30000;

Finres_c = Foreach fillndata Generate id, loan_amnt, int_rate, grade;

Dump Finres_c;

Store Finres_c into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_bc1.txt';

-----------------------------------------------------------------------------------------------

-- D: Get maximum number of loan given to which grade users (A-G).

Grpgrade = Group loandata by grade; 

cntgrade = Foreach Grpgrade Generate $0, COUNT($1) As cnt;

grpgradeall = Group cntgrade all;

getmax = Foreach grpgradeall Generate MAX(cntgrade.cnt) As maxcnt;

Finres_d = Filter cntgrade by ($1 == getmax.maxcnt);

Store Finres_d into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_d1.txt';

-----------------------------------------------------------------------------------------------

-- E: Highest loan amount given in that year with that Employee id and Employees annual income.

--Do this to get the max value in the tuple :

grpyr = Group loandata by SUBSTRING(issue_d,6,10);

maxloan = Foreach grpyr generate MAX(loandata.loan_amnt) as max_amt;

maxvaladd = Foreach grpyr generate flatten(loandata), MAX(loandata.loan_amnt) as max_amt;

-- This to get the final result

grpyrfn = Group maxvaladd by SUBSTRING(issue_d,6,10);

Finres_e = Foreach grpyrfn { maxrow = Filter maxvaladd by $2==$52; Generate group, maxrow.loan_amnt, maxrow.id, maxrow.annual_inc;};


Store Finres_e into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_e2.txt';

-----------------------------------------------------------------------------------------------

-- F: Get the total number of loans with loan id and load amount which are all having loan status as Late?

fillate = Filter loandata By (SUBSTRING(loan_status,0,4) == 'Late');

Grplate = Group fillate by loan_status;

latecnt = Foreach Grplate Generate COUNT($1);

Finres_f = Foreach fillate generate loan_status, id, loan_amnt;

Store Finres_f into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_f1.txt';


-----------------------------------------------------------------------------------------------

-- G: Average loan interest rate with 60 month term and 36 month term.

Grpmonth = Group loandata by term; 

Finres_g = Foreach Grpmonth Generate $0, AVG(loandata.int_rate);

Store Finres_g into '/home/edureka/learning/loan_project/LoanData_Results/Loanres_g1.txt';
