#!/bin/bash

## Account.csv
input_file1="Account.csv"
output_file1="Account_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8 col9 col10
do
  new_col1="Account|$col1"
  echo "$new_col1@$col2@$col2@$col3@$col4@$col5@$col6@$col7@$col8@$col9@$col10@$col1" >> "$output_file1"
done < "$input_file1"
echo "处理完成。已创建 $output_file1 文件。"


##AccountRepayLoan.csv
input_file2="AccountRepayLoan.csv"
output_file2="AccountRepayLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Account|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file2"
done < "$input_file2"
echo "处理完成。已创建 $output_file2 文件。"


##AccountTransferAccount.csv
input_file3="AccountTransferAccount.csv"
output_file3="AccountTransferAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7
do
  new_col1="Account|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4@$col5@$col6@$col7" >> "$output_file3"
done < "$input_file3"
echo "处理完成。已创建 $output_file3 文件。"


##AccountWithdrawAccount.csv
input_file4="AccountWithdrawAccount.csv"
output_file4="AccountWithdrawAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Account|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file4"
done < "$input_file4"
echo "处理完成。已创建 $output_file4 文件。"


##CompanyApplyLoan.csv
input_file5="CompanyApplyLoan.csv"
output_file5="CompanyApplyLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file5"
done < "$input_file5"
echo "处理完成。已创建 $output_file5 文件。"


##Company.csv
input_file6="Company.csv"
output_file6="Company_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8 col9
do
  new_col1="Company|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col7@$col8@$col9" >> "$output_file6"
done < "$input_file6"
echo "处理完成。已创建 $output_file6 文件。"


##CompanyGuaranteeCompany.csv
input_file7="CompanyGuaranteeCompany.csv"
output_file7="CompanyGuaranteeCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file7"
done < "$input_file7"
echo "处理完成。已创建 $output_file7 文件。"



##CompanyInvestCompany.csv
input_file8="CompanyInvestCompany.csv"
output_file8="CompanyInvestCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Company|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file8"
done < "$input_file8"
echo "处理完成。已创建 $output_file8 文件。"



##CompanyOwnAccount.csv
input_file9="CompanyOwnAccount.csv"
output_file9="CompanyOwnAccount_new.csv"
while IFS='|' read -r col1 col2 col3
do
  new_col1="Company|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3" >> "$output_file9"
done < "$input_file9"
echo "处理完成。已创建 $output_file9 文件。"


##Loan.csv
input_file10="Loan.csv"
output_file10="Loan_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6
do
  new_col1="Loan|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6" >> "$output_file10"
done < "$input_file10"
echo "处理完成。已创建 $output_file10 文件。"



##LoanDepositAccount.csv
input_file11="LoanDepositAccount.csv"
output_file11="LoanDepositAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Loan|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file11"
done < "$input_file11"
echo "处理完成。已创建 $output_file11 文件。"



##Medium.csv
input_file12="Medium.csv"
output_file12="Medium_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6
do
  new_col1="Medium|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col1" >> "$output_file12"
done < "$input_file12"
echo "处理完成。已创建 $output_file12 文件。"



##MediumSignInAccount.csv
input_file13="MediumSignInAccount.csv"
output_file13="MediumSignInAccount_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Medium|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file13"
done < "$input_file13"
echo "处理完成。已创建 $output_file13 文件。"


##PersonApplyLoan.csv
input_file14="PersonApplyLoan.csv"
output_file14="PersonApplyLoan_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Loan|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file14"
done < "$input_file14"
echo "处理完成。已创建 $output_file14 文件。"



##Person.csv
input_file15="Person.csv"
output_file15="Person_new.csv"
while IFS='|' read -r col1 col2 col3 col4 col5 col6 col7 col8
do
  new_col1="Person|$col1"
  echo "$new_col1@$col2@$col3@$col4@$col5@$col6@$col7@$col8" >> "$output_file15"
done < "$input_file15"
echo "处理完成。已创建 $output_file15 文件。"



##PersonGuaranteePerson.csv
input_file17="PersonGuaranteePerson.csv"
output_file17="PersonGuaranteePerson_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Person|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file17"
done < "$input_file17"
echo "处理完成。已创建 $output_file17 文件。"


##PersonInvestCompany.csv
input_file18="PersonInvestCompany.csv"
output_file18="PersonInvestCompany_new.csv"
while IFS='|' read -r col1 col2 col3 col4
do
  new_col1="Person|$col1"
  new_col2="Company|$col2"
  echo "$new_col1@$new_col2@$col3@$col4" >> "$output_file18"
done < "$input_file18"
echo "处理完成。已创建 $output_file18 文件。"



##PersonOwnAccount.csv
input_file19="PersonOwnAccount.csv"
output_file19="PersonOwnAccount_new.csv"
while IFS='|' read -r col1 col2 col3
do
  new_col1="Person|$col1"
  new_col2="Account|$col2"
  echo "$new_col1@$new_col2@$col3" >> "$output_file19"
done < "$input_file19"
echo "处理完成。已创建 $output_file19 文件。"

