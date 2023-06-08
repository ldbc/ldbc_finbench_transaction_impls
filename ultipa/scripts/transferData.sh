#!/bin/bash

## Account.csv
input_file1="../data/sf/snapshot/Account.csv"
output_file1="../data/sf/snapshot/Account_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8 col9 col10
do
  new_col1="Account|$col1"
  echo "$new_col1@$col2@$col2@$col3@$col4@$col5@$col6@$col7@$col8@$col9@$col10@$col1" >> "$output_file1"
done < "$input_file1"

##AccountRepayLoan.csv
input_file2="../data/sf/snapshot/AccountRepayLoan.csv"
output_file2="../data/sf/snapshot/AccountRepayLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Account|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file2"
done < "$input_file2"

##AccountTransferAccount.csv
input_file3="../data/sf/snapshot/AccountTransferAccount.csv"
output_file3="../data/sf/snapshot/AccountTransferAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7
do
  new_col1="Account|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4@$col5@$col6@$col7" >> "$output_file3"
done < "$input_file3"

##AccountWithdrawAccount.csv
input_file4="../data/sf/snapshot/AccountWithdrawAccount.csv"
output_file4="../data/sf/snapshot/AccountWithdrawAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Account|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file4"
done < "$input_file4"

##CompanyApplyLoan.csv
input_file5="../data/sf/snapshot/CompanyApplyLoan.csv"
output_file5="../data/sf/snapshot/CompanyApplyLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file5"
done < "$input_file5"

##Company.csv
input_file6="../data/sf/snapshot/Company.csv"
output_file6="../data/sf/snapshot/Company_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8 col9
do
  new_col1="Company|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col7@$col8@$col9" >> "$output_file6"
done < "$input_file6"

##CompanyGuaranteeCompany.csv
input_file7="../data/sf/snapshot/CompanyGuaranteeCompany.csv"
output_file7="../data/sf/snapshot/CompanyGuaranteeCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file7"
done < "$input_file7"

##CompanyInvestCompany.csv
input_file8="../data/sf/snapshot/CompanyInvestCompany.csv"
output_file8="../data/sf/snapshot/CompanyInvestCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file8"
done < "$input_file8"

##CompanyOwnAccount.csv
input_file9="../data/sf/snapshot/CompanyOwnAccount.csv"
output_file9="../data/sf/snapshot/CompanyOwnAccount_new.csv"
while IFS='|' read -r col1 col2 col3
do
  new_col1="Company|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3" >> "$output_file9"
done < "$input_file9"


##Loan.csv
input_file10="../data/sf/snapshot/Loan.csv"
output_file10="../data/sf/snapshot/Loan_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6
do
  new_col1="Loan|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6" >> "$output_file10"
done < "$input_file10"

##LoanDepositAccount.csv
input_file11="../data/sf/snapshot/LoanDepositAccount.csv"
output_file11="../data/sf/snapshot/LoanDepositAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Loan|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file11"
done < "$input_file11"

##Medium.csv
input_file12="../data/sf/snapshot/Medium.csv"
output_file12="../data/sf/snapshot/Medium_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6
do
  new_col1="Medium|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col1" >> "$output_file12"
done < "$input_file12"

##MediumSignInAccount.csv
input_file13="../data/sf/snapshot/MediumSignInAccount.csv"
output_file13="../data/sf/snapshot/MediumSignInAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Medium|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file13"
done < "$input_file13"

##PersonApplyLoan.csv
input_file14="../data/sf/snapshot/PersonApplyLoan.csv"
output_file14="../data/sf/snapshot/PersonApplyLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file14"
done < "$input_file14"

##Person.csv
input_file15="../data/sf/snapshot/Person.csv"
output_file15="../data/sf/snapshot/Person_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8
do
  new_col1="Person|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col7@$col8" >> "$output_file15"
done < "$input_file15"

##PersonGuaranteePerson.csv
input_file17="../data/sf/snapshot/PersonGuaranteePerson.csv"
output_file17="../data/sf/snapshot/PersonGuaranteePerson_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Person|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file17"
done < "$input_file17"

##PersonInvestCompany.csv
input_file18="../data/sf/snapshot/PersonInvestCompany.csv"
output_file18="../data/sf/snapshot/PersonInvestCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file18"
done < "$input_file18"

##PersonOwnAccount.csv
input_file19="../data/sf/snapshot/PersonOwnAccount.csv"
output_file19="../data/sf/snapshot/PersonOwnAccount_new.csv"
while IFS='|' read -r col1 col2 col3
do
  new_col1="Person|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3" >> "$output_file19"
done < "$input_file19"
