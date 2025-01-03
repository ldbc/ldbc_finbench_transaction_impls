#!/bin/bash

cwd=$(cd $(dirname $0) && pwd)
cd $cwd

function set_udf_put () {
  gadmin config set GSQL.UDF.EnablePutTokenBank $1
  gadmin config set GSQL.UDF.EnablePutExpr $1
  gadmin config apply -y
  gadmin restart -y --wait-online
}

udfEnabled=$(gadmin config get GSQL.UDF.EnablePutExpr)
tokenBankEnabled=$(gadmin config get GSQL.UDF.EnablePutTokenBank)
if [[ $udfEnabled == "false" || $tokenBankEnabled == "false"  ]]; then
  set_udf_put true
fi

gsql '
DROP ALL

CREATE VERTEX Person(PRIMARY_ID id UINT, name STRING, isBlocked BOOL, createTime UINT, gender STRING, birthday UINT, country STRING, city STRING)WITH primary_id_as_attribute="TRUE"
CREATE VERTEX Account(PRIMARY_ID id UINT, createTime UINT, isBlocked BOOL, accountType STRING, nickName STRING, phoneNumber STRING, email STRING, freqLoginType STRING, lastLoginTime UINT, accountLevel STRING) WITH primary_id_as_attribute="TRUE"
CREATE VERTEX Company(PRIMARY_ID id UINT, name STRING, isBlocked BOOL, createTime UINT, country STRING, city STRING, business STRING, description STRING, url STRING) WITH primary_id_as_attribute="TRUE"
CREATE VERTEX Loan(PRIMARY_ID id UINT, loanAmount DOUBLE, balance DOUBLE, usage STRING, interestRate FLOAT) WITH primary_id_as_attribute="TRUE"
CREATE VERTEX Medium(PRIMARY_ID id UINT, mediumType STRING, createTime UINT, isBlocked BOOL, lastLoginTime UINT, riskLevel STRING) WITH primary_id_as_attribute="TRUE"

# single-edge
CREATE DIRECTED EDGE own(From Person|Company, To Account, timestamp UINT) WITH REVERSE_EDGE="own_REVERSE"
CREATE DIRECTED EDGE invest(From Person|Company, To Company, timestamp UINT, ratio FLOAT) WITH REVERSE_EDGE="invest_REVERSE"
CREATE DIRECTED EDGE apply(From Person|Company, To Loan, timestamp UINT, organization STRING) WITH REVERSE_EDGE="apply_REVERSE"
CREATE DIRECTED EDGE guarantee(FROM Person, TO Person | FROM Company, TO Company, timestamp UINT, relationship STRING) WITH REVERSE_EDGE="guarantee_REVERSE"

# multi-edge
CREATE DIRECTED EDGE transfer(From Account, To Account, DISCRIMINATOR(timestamp UINT), orderNumber STRING, amount DOUBLE, comment STRING, payType STRING, goodsType STRING) WITH REVERSE_EDGE="transfer_REVERSE"
CREATE DIRECTED EDGE deposit(From Loan, To Account, DISCRIMINATOR(timestamp UINT), amount DOUBLE) WITH REVERSE_EDGE="deposit_REVERSE"
CREATE DIRECTED EDGE repay(From Account, To Loan, DISCRIMINATOR(timestamp UINT), amount DOUBLE) WITH REVERSE_EDGE="repay_REVERSE"
CREATE DIRECTED EDGE withdraw(From Account, To Account, DISCRIMINATOR(timestamp UINT), amount DOUBLE) WITH REVERSE_EDGE="withdraw_REVERSE"
CREATE DIRECTED EDGE signIn(From Medium, To Account, DISCRIMINATOR(timestamp UINT), location STRING) WITH REVERSE_EDGE="signIn_REVERSE"

CREATE GRAPH ldbc_fin(*)
'

gsql 'PUT ExprFunctions FROM "./ExprFunctions.hpp"'
gsql 'PUT TokenBank FROM "./TokenBank.cpp"'

gsql -g ldbc_fin '
CREATE LOADING JOB load_fin_snapshot FOR GRAPH ldbc_fin{
  DEFINE FILENAME file_Person;
  DEFINE FILENAME file_Account;
  DEFINE FILENAME file_Company;
  DEFINE FILENAME file_Loan;
  DEFINE FILENAME file_Medium;

  DEFINE FILENAME file_Company_Own_Account;
  DEFINE FILENAME file_Person_Own_Account;
  DEFINE FILENAME file_Company_Invest;
  DEFINE FILENAME file_Person_Invest;
  DEFINE FILENAME file_Person_Apply_Loan;
  DEFINE FILENAME file_Company_Apply_Loan;
  DEFINE FILENAME file_Company_Guarantee;
  DEFINE FILENAME file_Person_Guarantee;

  DEFINE FILENAME file_transfer;
  DEFINE FILENAME file_deposit;
  DEFINE FILENAME file_repay;
  DEFINE FILENAME file_withdraw;
  DEFINE FILENAME file_signIn;

  Load file_Person
    TO VERTEX Person values($0, $1, $2, ToMiliSeconds($3), $4, ToMiliSeconds($5), $6, $7) USING header="true", separator="|";
  Load file_Account
    TO VERTEX Account values($0, ToMiliSeconds($1), $2, $3, $4, $5, $6, $7, $8, $9) USING header="true", separator="|";
  Load file_Company
    TO VERTEX Company values($0, $1, $2, ToMiliSeconds($3), $4, $5, $6, $7, $8) USING header="true", separator="|";
  Load file_Loan
    TO VERTEX Loan values($0, $1, $2, $4, $5) USING header="true", separator="|";
  Load file_Medium
    TO VERTEX Medium values($0, $1, ToMiliSeconds($3), $2, $4, $5) USING header="true", separator="|";

  LOAD file_Company_Own_Account
    TO EDGE own VALUES($0 Company, $1, ToMiliSeconds($2)) USING header="true", separator="|";
  LOAD file_Person_Own_Account
    TO EDGE own VALUES($0 Person, $1, ToMiliSeconds($2)) USING header="true", separator="|";
  LOAD file_Company_Invest
    TO EDGE invest VALUES($0 Company, $1, ToMiliSeconds($3), $2) USING header="true", separator="|";
  LOAD file_Person_Invest
    TO EDGE invest VALUES($0 Person, $1, ToMiliSeconds($3), $2) USING header="true", separator="|";
  LOAD file_Person_Apply_Loan
    TO EDGE apply VALUES($0 Person, $1, ToMiliSeconds($2), $3) USING header="true", separator="|";
  LOAD file_Company_Apply_Loan
    TO EDGE apply VALUES($0 Company, $1, ToMiliSeconds($2), $3) USING header="true", separator="|";
  LOAD file_Company_Guarantee
    TO EDGE guarantee VALUES($0 Company, $1 Company, ToMiliSeconds($2), $3) USING header="true", separator="|";
  LOAD file_Person_Guarantee
    TO EDGE guarantee VALUES($0 Person, $1 Person, ToMiliSeconds($2), $3) USING header="true", separator="|";

  LOAD file_transfer
    TO EDGE transfer VALUES($0, $1, ToMiliSeconds($3), $4, $2, $5, $6, $7) USING header="true", separator="|";
  LOAD file_deposit
    TO EDGE deposit VALUES($0, $1, ToMiliSeconds($3), $2) USING header="true", separator="|";
  LOAD file_repay
    TO EDGE repay VALUES($0, $1, ToMiliSeconds($3), $2) USING header="true", separator="|";
  LOAD file_withdraw
    TO EDGE withdraw VALUES($0, $1, ToMiliSeconds($3), $2) USING header="true", separator="|";
  LOAD file_signIn
    TO EDGE signIn VALUES($0, $1, ToMiliSeconds($2), $3) USING header="true", separator="|";
}'