# Logbook / Dashboard Processor
This project consolidates a multi-step data processing workflow for Monthly Data Processing (previously a Monthly Loogbook for Excel) in Power BI for Procurement and Logistic Division MAA Group.

## Folder Structure
```
PROCUREMENT-PROCESSOR/
├── consolidated_data/         # POWER BI SOURCE: Central database for Power BI reports
│   └── data.xlsx               # Primary data source/table for BI consumption
├── data/
│   ├── po_entry/              # INPUT: Main raw PO entry files
│   │   ├── PO Entry List.xlsx   # Main input data source (from CPS)
│   │   └── PO Manual PROCESS.xlsx # Manual PO, consolidate from Synology
│   ├── processed/             # OUTPUT: Folder for processed data output
│   │   └── output.xlsx          # Final output file (saved by the pipeline)
│   └── reference/             # LOOKUP: Static lookup tables for normalization
│       ├── cost_saving.xlsx
│       ├── jasa_service.xlsx
│       ├── logistic_freight.xlsx
│       ├── non_workdays.xlsx    # Used for nonworkdays exclusion
│       ├── normalisasi.xlsx     # Used for RFM Normalization
│       ├── ontime_normalisasi.xlsx # Used for PO Ontime Normalization
│       ├── pulau.xlsx # Pulau lookup
│       └── wilayah.xlsx # Wilayah lookup
├── output/                    # Legacy or secondary output folder
└── src/                       # SOURCE CODE ROOT
    ├── legacy/                # Archive for old scripts/notebooks
    │   └── weekly logbook consolidated.ipynb # All-in-one notebook, could be used for bug fix
    ├── notebooks/             # Execution environment
    │   └── orchestrator.ipynb # Pipeline Entry Point
    ├── pipeline/              # CORE LOGIC PACKAGE
    │   ├── __init__.py          # Marks 'pipeline' as a Python package
    │   └── data_helper.py       # (Helper functions, rules, freight logic)
    │   └── data_loader.py       # (Path constants, data ingestion)
    │   └── processing_steps.py  # (Sequential transformation workflow)
    └── README.md              # Project documentation
```

## Architecture Overview
The pipeline is split into three main modules:
| Module | Responsibility | 
| ------------- | ------------- |
|` data_loader.py`  | Data ingestion and path management. `PO Entry List` & `data/reference`  |
|` data_helper.py`| Core Business Rules & Utilities. Encapsulates all non-sequential logic, such as complex string parsers, date difference calculations, team definitions, and efficiency-optimized dictionary mappings. eg.`VALUE`,  `LOC`, `DEPARTMENT_`, `DIVISI`, & `Lebaran Exclusion Date`  |
|`processing_steps.py` | Sequential Workflow Engine. Run all of the processing from `data_helper.py` and calculate other business metrics |
|`orchestrator.ipynb` | Pipeline Entry Point. Serves as execution |

## Data Processing Highlight (Key Metrics)
The pipeline performs the following complex calculations:
+ Custom Date Difference: Calculates working days between two dates (`PR - PO`, `PO - RPO`) while excluding designated national holidays `lebaran_dates`.

+ Dynamic Lead Time (TIME DATE): Calculates the required delivery deadline based on `LOC`, `Item Category`, and customized normalization rules `timedate_normalized_df`.

+ Purchasing Status: Classifies procurement performance `STATUS_PURCHASING` based on team-specific lead time thresholds (HO: 5 days, Site: 3 days).

+ Freight Determination: Determines `LOGISTIC_FREIGHT` using a hierarchy that prioritizes vendor-specific mapping before checking general rules, using efficient pre-computed maps.

## Prerequisites
Python (3.10+) with the following libraries installed
```
pip install pandas numpy openpyxl
```

This project was created with 3.12.10


## Contributor
+ Ferdinand - Data & System Analyst (2022 - 2024)
+ [Muhammad Rifqy Irfanto - Data & System Analyst](https://github.com/rifqyirfanto21) (2024 - 2025)
+ [Fajar Amry Milhan - Data & System Analyst](https://github.com/nagabonar27) (2024 - )
+ [Laurensius Kristianto Adi - Data & System Analyst](https://github.com/laurensiusadii) (2024 - )