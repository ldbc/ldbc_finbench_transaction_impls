#!/bin/bash

DATA_SIZE=${1:-1}

data_root="$(dirname "$0")/../data/sf${DATA_SIZE}/snapshot"

gsql -g ldbc_fin "
RUN LOADING JOB load_fin_snapshot USING
  file_Person=\"${data_root}/Person.csv\",
  file_Account=\"${data_root}/Account.csv\",
  file_Company=\"${data_root}/Company.csv\",
  file_Loan=\"${data_root}/Loan.csv\",
  file_Medium=\"${data_root}/Medium.csv\",
  file_Company_Own_Account=\"${data_root}/CompanyOwnAccount.csv\",
  file_Person_Own_Account=\"${data_root}/PersonOwnAccount.csv\",
  file_Company_Invest=\"${data_root}/CompanyInvestCompany.csv\",
  file_Person_Invest=\"${data_root}/PersonInvestCompany.csv\",
  file_Person_Apply_Loan=\"${data_root}/PersonApplyLoan.csv\",
  file_Company_Apply_Loan=\"${data_root}/CompanyApplyLoan.csv\",
  file_Company_Guarantee=\"${data_root}/CompanyGuaranteeCompany.csv\",
  file_Person_Guarantee=\"${data_root}/PersonGuaranteePerson.csv\",
  file_transfer=\"${data_root}/AccountTransferAccount.csv\",
  file_deposit=\"${data_root}/LoanDepositAccount.csv\",
  file_repay=\"${data_root}/AccountRepayLoan.csv\",
  file_withdraw=\"${data_root}/AccountWithdrawAccount.csv\",
  file_signIn=\"${data_root}/MediumSignInAccount.csv\"
"