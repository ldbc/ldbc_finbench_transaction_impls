# Interactively ask for column type and output to file
import os
from pathlib import Path

header_dir = Path("orig_headers")
intermediate_dir = Path("intermediate")
new_header_dir = Path("headers")
new_header_dir.mkdir(exist_ok=True)

# Keep relation endpoints explicit. Filename-based inference is brittle for
# multi-word verbs such as MediumSignInAccount.
RELATION_ENDPOINTS = {
    "AccountRepayLoan.csv": ("Account", "Loan"),
    "AccountTransferAccount.csv": ("Account", "Account"),
    "AccountWithdrawAccount.csv": ("Account", "Account"),
    "CompanyApplyLoan.csv": ("Company", "Loan"),
    "CompanyGuaranteeCompany.csv": ("Company", "Company"),
    "CompanyInvestCompany.csv": ("Company", "Company"),
    "CompanyOwnAccount.csv": ("Company", "Account"),
    "LoanDepositAccount.csv": ("Loan", "Account"),
    "MediumSignInAccount.csv": ("Medium", "Account"),
    "PersonApplyLoan.csv": ("Person", "Loan"),
    "PersonGuaranteePerson.csv": ("Person", "Person"),
    "PersonInvestCompany.csv": ("Person", "Company"),
    "PersonOwnAccount.csv": ("Person", "Account"),
}


def build_header(filename: str) -> str:
    node_name = ""
    src_name = ""
    dst_name = ""
    pieces = []
    schema_path = intermediate_dir / filename

    with open(schema_path, "r") as f:
        lines = [line.split() for line in f.readlines() if line.split()]

    if not lines:
        raise ValueError(f"Empty intermediate schema file: {schema_path}")

    for i, line_ls in enumerate(lines):
        col_name, col_type = line_ls[0], line_ls[1]
        piece = "id:ID" if i == 0 and col_type == "ID" else f"{col_name}:{col_type}"

        if i == 0:
            if col_type == "ID":
                node_name = filename[:filename.find(".csv")]
                piece += f"({node_name})"
            else:
                if filename not in RELATION_ENDPOINTS:
                    raise ValueError(
                        f"Missing explicit endpoint mapping for relation file: {filename}"
                    )
                src_name, dst_name = RELATION_ENDPOINTS[filename]
                piece += f"({src_name})"
        elif i == 1 and dst_name:
            piece += f"({dst_name})"

        pieces.append(piece)

    return "|".join(pieces)


for filename in sorted(os.listdir(header_dir)):
    if not filename.endswith(".csv"):
        continue
    header = build_header(filename)
    with open(new_header_dir / filename, "w") as fout:
        fout.write(header + "\n")
