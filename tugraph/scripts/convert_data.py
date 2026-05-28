from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


SOURCE_COLUMNS = {
    "Person.csv": [
        "personId",
        "personName",
        "isBlocked",
        "createTime",
        "gender",
        "birthday",
        "country",
        "city",
    ],
    "Account.csv": [
        "accountId",
        "createTime",
        "isBlocked",
        "accountType",
        "nickname",
        "phonenum",
        "email",
        "freqLoginType",
        "lastLoginTime",
        "accountLevel",
    ],
    "Medium.csv": [
        "mediumId",
        "mediumType",
        "isBlocked",
        "createTime",
        "lastLoginTime",
        "riskLevel",
    ],
    "Loan.csv": [
        "loanId",
        "loanAmount",
        "balance",
        "createTime",
        "loanUsage",
        "interestRate",
    ],
    "Company.csv": [
        "companyId",
        "companyName",
        "isBlocked",
        "createTime",
        "country",
        "city",
        "business",
        "description",
        "url",
    ],
    "AccountTransferAccount.csv": [
        "fromId",
        "toId",
        "amount",
        "createTime",
        "orderNum",
        "comment",
        "payType",
        "goodsType",
    ],
    "AccountWithdrawAccount.csv": [
        "fromId",
        "toId",
        "fromType",
        "toType",
        "amount",
        "createTime",
        "comment",
    ],
    "AccountRepayLoan.csv": [
        "accountId",
        "loanId",
        "amount",
        "createTime",
        "comment",
    ],
    "LoanDepositAccount.csv": [
        "loanId",
        "accountId",
        "amount",
        "createTime",
        "comment",
    ],
    "MediumSignInAccount.csv": [
        "mediumId",
        "accountId",
        "createTime",
        "location",
        "comment",
    ],
    "PersonInvestCompany.csv": [
        "investorId",
        "companyId",
        "ratio",
        "createTime",
        "comment",
    ],
    "CompanyInvestCompany.csv": [
        "investorId",
        "companyId",
        "ratio",
        "createTime",
        "comment",
    ],
    "PersonApplyLoan.csv": [
        "personId",
        "loanId",
        "loanAmount",
        "createTime",
        "org",
        "comment",
    ],
    "CompanyApplyLoan.csv": [
        "companyId",
        "loanId",
        "loanAmount",
        "createTime",
        "org",
        "comment",
    ],
    "PersonGuaranteePerson.csv": [
        "fromId",
        "toId",
        "createTime",
        "relation",
        "comment",
    ],
    "CompanyGuaranteeCompany.csv": [
        "fromId",
        "toId",
        "createTime",
        "relation",
        "comment",
    ],
    "CompanyOwnAccount.csv": [
        "companyId",
        "accountId",
        "createTime",
        "comment",
    ],
    "PersonOwnAccount.csv": [
        "personId",
        "accountId",
        "createTime",
        "comment",
    ],
}

IMPORT_CONF_COLUMN_MAP = {
    "Person.csv": [
        ("personId", "id"),
        ("personName", "name"),
        ("isBlocked", "isBlocked"),
        ("createTime", "createTime"),
        ("gender", "gender"),
        ("birthday", "birthday"),
        ("country", "country"),
        ("city", "city"),
    ],
    "Account.csv": [
        ("accountId", "id"),
        ("createTime", "createTime"),
        ("isBlocked", "isBlocked"),
        ("accountType", "type"),
        ("nickname", "nickname"),
        ("phonenum", "phonenum"),
        ("email", "email"),
        ("freqLoginType", "freqLoginType"),
        ("lastLoginTime", "lastLoginTime"),
        ("accountLevel", "accountLevel"),
    ],
    "Medium.csv": [
        ("mediumId", "id"),
        ("mediumType", "type"),
        ("isBlocked", "isBlocked"),
        ("createTime", "createTime"),
        ("lastLoginTime", "lastLoginTime"),
        ("riskLevel", "riskLevel"),
    ],
    "Loan.csv": [
        ("loanId", "id"),
        ("loanAmount", "loanAmount"),
        ("balance", "balance"),
        ("createTime", "createTime"),
        ("loanUsage", "loanUsage"),
        ("interestRate", "interestRate"),
    ],
    "Company.csv": [
        ("companyId", "id"),
        ("companyName", "name"),
        ("isBlocked", "isBlocked"),
        ("createTime", "createTime"),
        ("country", "country"),
        ("city", "city"),
        ("business", "business"),
        ("description", "description"),
        ("url", "url"),
    ],
    "AccountTransferAccount.csv": [
        ("fromId", "SRC_ID"),
        ("toId", "DST_ID"),
        ("amount", "amount"),
        ("createTime", "timestamp"),
        ("orderNum", "orderNum"),
        ("comment", "comment"),
        ("payType", "payType"),
        ("goodsType", "goodsType"),
    ],
    "AccountWithdrawAccount.csv": [
        ("fromId", "SRC_ID"),
        ("toId", "DST_ID"),
        ("amount", "amount"),
        ("createTime", "timestamp"),
    ],
    "AccountRepayLoan.csv": [
        ("accountId", "SRC_ID"),
        ("loanId", "DST_ID"),
        ("amount", "amount"),
        ("createTime", "timestamp"),
    ],
    "LoanDepositAccount.csv": [
        ("loanId", "SRC_ID"),
        ("accountId", "DST_ID"),
        ("amount", "amount"),
        ("createTime", "timestamp"),
    ],
    "MediumSignInAccount.csv": [
        ("mediumId", "SRC_ID"),
        ("accountId", "DST_ID"),
        ("createTime", "timestamp"),
        ("location", "location"),
    ],
    "PersonInvestCompany.csv": [
        ("investorId", "SRC_ID"),
        ("companyId", "DST_ID"),
        ("ratio", "ratio"),
        ("createTime", "timestamp"),
    ],
    "CompanyInvestCompany.csv": [
        ("investorId", "SRC_ID"),
        ("companyId", "DST_ID"),
        ("ratio", "ratio"),
        ("createTime", "timestamp"),
    ],
    "PersonApplyLoan.csv": [
        ("personId", "SRC_ID"),
        ("loanId", "DST_ID"),
        ("createTime", "timestamp"),
        ("org", "org"),
    ],
    "CompanyApplyLoan.csv": [
        ("companyId", "SRC_ID"),
        ("loanId", "DST_ID"),
        ("createTime", "timestamp"),
        ("org", "org"),
    ],
    "PersonGuaranteePerson.csv": [
        ("fromId", "SRC_ID"),
        ("toId", "DST_ID"),
        ("createTime", "timestamp"),
        ("relation", "relation"),
    ],
    "CompanyGuaranteeCompany.csv": [
        ("fromId", "SRC_ID"),
        ("toId", "DST_ID"),
        ("createTime", "timestamp"),
        ("relation", "relation"),
    ],
    "CompanyOwnAccount.csv": [
        ("companyId", "SRC_ID"),
        ("accountId", "DST_ID"),
        ("createTime", "timestamp"),
    ],
    "PersonOwnAccount.csv": [
        ("personId", "SRC_ID"),
        ("accountId", "DST_ID"),
        ("createTime", "timestamp"),
    ],
}

TIMESTAMP_OUTPUT_COLUMNS = {
    "Account.csv": {"createTime"},
    "AccountTransferAccount.csv": {"timestamp"},
    "AccountWithdrawAccount.csv": {"timestamp"},
    "AccountRepayLoan.csv": {"timestamp"},
    "LoanDepositAccount.csv": {"timestamp"},
    "MediumSignInAccount.csv": {"timestamp"},
    "PersonInvestCompany.csv": {"timestamp"},
    "CompanyInvestCompany.csv": {"timestamp"},
    "PersonApplyLoan.csv": {"timestamp"},
    "CompanyApplyLoan.csv": {"timestamp"},
    "PersonGuaranteePerson.csv": {"timestamp"},
    "CompanyGuaranteeCompany.csv": {"timestamp"},
    "CompanyOwnAccount.csv": {"timestamp"},
    "PersonOwnAccount.csv": {"timestamp"},
}

TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
)

OUTPUT_DELIMITER = "|"


def detect_delimiter(first_line: str) -> str:
    return "|" if "|" in first_line else ","


def to_epoch_millis(value: str) -> str:
    if value is None:
        return ""

    cleaned = value.strip()
    if not cleaned or cleaned.isdigit():
        return cleaned

    for fmt in TIMESTAMP_FORMATS:
        try:
            parsed = datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
            return str(int(parsed.timestamp() * 1000))
        except ValueError:
            continue

    return cleaned


def validate_headers(filename: str, actual_headers: List[str]) -> None:
    expected_headers = SOURCE_COLUMNS[filename]
    missing = [column for column in expected_headers if column not in actual_headers]
    extra = [column for column in actual_headers if column not in expected_headers]

    if not missing and not extra:
        return

    details = []
    if missing:
        details.append(f"missing columns: {missing}")
    if extra:
        details.append(f"unexpected columns: {extra}")

    raise ValueError(f"{filename} header mismatch ({'; '.join(details)})")


def build_original_row(actual_headers: List[str], row: Dict[str, str]) -> Dict[str, str]:
    return {column: row.get(column, "") for column in actual_headers}


def build_import_conf_row(filename: str, row: Dict[str, str]) -> Dict[str, str]:
    output_row: Dict[str, str] = {}
    timestamp_columns = TIMESTAMP_OUTPUT_COLUMNS.get(filename, set())

    for source_column, target_column in IMPORT_CONF_COLUMN_MAP[filename]:
        value = row.get(source_column, "")
        if target_column in timestamp_columns:
            value = to_epoch_millis(value)
        output_row[target_column] = value

    return output_row


def convert_file(input_path: Path, output_path: Path, mode: str) -> None:
    filename = input_path.name

    with input_path.open("r", encoding="utf-8-sig", newline="") as source_handle:
        first_line = source_handle.readline()
        source_handle.seek(0)

        reader = csv.DictReader(source_handle, delimiter=detect_delimiter(first_line))
        actual_headers = reader.fieldnames or []

        if not actual_headers:
            raise ValueError(f"{filename} does not contain a header row")

        validate_headers(filename, actual_headers)

        if mode == "original":
            output_headers = actual_headers
        elif mode == "import_conf":
            output_headers = [
                target_column
                for _, target_column in IMPORT_CONF_COLUMN_MAP[filename]
            ]
        else:
            raise ValueError(f"unsupported mode: {mode}")

        with output_path.open("w", encoding="utf-8", newline="") as target_handle:
            writer = csv.DictWriter(
                target_handle,
                fieldnames=output_headers,
                delimiter=OUTPUT_DELIMITER,
            )
            writer.writeheader()

            for row in reader:
                if mode == "original":
                    output_row = build_original_row(actual_headers, row)
                else:
                    output_row = build_import_conf_row(filename, row)
                writer.writerow(output_row)


def convert_dataset(input_dir: Path, output_dir: Path, mode: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for filename in SOURCE_COLUMNS:
        input_path = input_dir / filename
        if not input_path.exists():
            print(f"[skip] {filename}: file not found in {input_dir}")
            continue

        output_path = output_dir / filename
        convert_file(input_path, output_path, mode)
        print(f"[ok] {filename} -> {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert source CSV files into import.conf-compatible CSV files."
    )
    parser.add_argument(
        "--input-dir",
        default=".",
        help="Directory containing the raw source CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="clean_csv_import_conf",
        help="Directory where converted CSV files will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    convert_dataset(Path(args.input_dir), Path(args.output_dir), mode="import_conf")


if __name__ == "__main__":
    main()
