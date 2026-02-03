# Jack Wilson
# 2/3/2026
# This file creates or updates the driver prediction list file used for prediction a specific race

# --------------------------------------------------------------------------------
# Import modules

import os
import sys
import time
import warnings

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _close_workbook_xlwings(file_path):
    """
    If the workbook is open in Excel, connect via xlwings, save, and close.
    Often works when win32com path matching fails (e.g. OneDrive paths).
    No-op if xlwings not installed or file not open. Requires: pip install xlwings
    """
    try:
        import xlwings as xw
    except ImportError:
        return False
    path_abs = os.path.abspath(file_path)
    path_norm = os.path.normcase(path_abs)
    try:
        for app in xw.apps:
            for book in app.books:
                try:
                    fn = book.fullname
                    if fn and os.path.normcase(os.path.abspath(fn)) == path_norm:
                        book.save()
                        book.close()
                        time.sleep(0.3)
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _close_workbook_in_excel(file_path):
    """
    If the workbook is open in Excel (Windows), close it so the file can be read/written.
    Tries xlwings first (if installed), then win32com. Saves before closing.
    """
    if _close_workbook_xlwings(file_path):
        return
    try:
        import win32com.client
    except ImportError:
        return
    target_abs = os.path.abspath(file_path)
    target_norm = os.path.normcase(target_abs)
    target_name = os.path.basename(file_path)
    try:
        target_real = os.path.realpath(target_abs)
    except OSError:
        target_real = target_abs
    xl = None
    try:
        xl = win32com.client.GetActiveObject("Excel.Application")
    except Exception:
        try:
            xl = win32com.client.Dispatch("Excel.Application")
        except Exception:
            return
    if xl is None:
        return
    try:
        if xl.Workbooks.Count == 0:
            return
    except Exception:
        return
    to_close = []
    try:
        count = xl.Workbooks.Count
    except Exception:
        return
    try:
        for i in range(1, count + 1):
            wb = xl.Workbooks(i)
            try:
                fn = wb.FullName
                if not fn:
                    if wb.Name == target_name:
                        to_close.append(wb.Name)
                        break
                    continue
                wb_abs = os.path.abspath(fn)
                wb_norm = os.path.normcase(wb_abs)
                try:
                    wb_real = os.path.realpath(wb_abs)
                except OSError:
                    wb_real = wb_abs
                if wb_norm == target_norm or wb_real == target_real:
                    to_close.append(wb.Name)
                    break
                if wb.Name == target_name:
                    to_close.append(wb.Name)
                    break
            except Exception:
                continue
        if not to_close:
            return
        xl.DisplayAlerts = False
        for name in to_close:
            try:
                xl.Workbooks(name).Close(SaveChanges=True)
            except Exception:
                pass
        time.sleep(0.3)
    except Exception:
        pass
    finally:
        try:
            if xl is not None:
                xl.DisplayAlerts = True
        except Exception:
            pass


def create_driver_pred_list(output_path=None):
    """
    Create or overwrite driver_pred_list.xlsx with:
    - 'list' sheet: index 1-22, name/team columns with data validation from drivers/teams sheets
    - 'drivers' sheet: unique driver names from f1_data_pre_qual_clean.csv (no header)
    - 'teams' sheet: unique team names from f1_data_pre_qual_clean.csv (no header)
    Driver and team lists are refreshed from the CSV each time.
    If the workbook already exists, existing name/team values in the "list" sheet are preserved.
    """
    if output_path is None:
        output_path = os.path.join(current_dir, "driver_pred_list.xlsx")

    _close_workbook_in_excel(output_path)
    time.sleep(0.5)
    file_existed = os.path.isfile(output_path)

    csv_path = os.path.join(PROJECT_ROOT, "data", "final", "f1_data_pre_qual_clean.csv")
    df = pd.read_csv(csv_path, low_memory=False)

    drivers = sorted(df["driver_name"].dropna().unique().tolist())
    teams = sorted(df["team_name"].dropna().unique().tolist())

    wb = Workbook()
    wb.remove(wb.active)

    ws_list = wb.create_sheet("list", 0)
    ws_drivers = wb.create_sheet("drivers", 1)
    ws_teams = wb.create_sheet("teams", 2)

    # --- list sheet ---
    ws_list["A1"] = "index"
    for i in range(1, 23):
        ws_list.cell(row=i + 1, column=1, value=i)
    ws_list["B1"] = "name"
    ws_list["C1"] = "team"

    n_drivers = len(drivers)
    n_teams = len(teams)
    dv_name = DataValidation(
        type="list",
        formula1=f"drivers!$A$1:$A${n_drivers}",
        allow_blank=True,
        showErrorMessage=False,
        showDropDown=False,  # False = show dropdown arrow in Excel
    )
    dv_team = DataValidation(
        type="list",
        formula1=f"teams!$A$1:$A${n_teams}",
        allow_blank=True,
        showErrorMessage=False,
        showDropDown=False,  # False = show dropdown arrow in Excel
    )
    ws_list.add_data_validation(dv_name)
    ws_list.add_data_validation(dv_team)
    for row in range(2, 24):
        dv_name.add(f"B{row}")
        dv_team.add(f"C{row}")

    default_width = 10
    ws_list.column_dimensions["B"].width = default_width * 2
    ws_list.column_dimensions["C"].width = default_width * 2

    # --- drivers sheet (no header) ---
    for r, name in enumerate(drivers, start=1):
        ws_drivers.cell(row=r, column=1, value=name)

    # --- teams sheet (no header) ---
    for r, name in enumerate(teams, start=1):
        ws_teams.cell(row=r, column=1, value=name)

    # Preserve existing name/team entries on "list" sheet if file already exists
    max_retries = 3
    retry_delay = 2
    last_error = None
    for attempt in range(max_retries):
        try:
            if file_existed:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning, message=".*Data Validation.*")
                    existing_wb = load_workbook(output_path, read_only=False, data_only=True)
                if "list" in existing_wb.sheetnames:
                    existing_list = existing_wb["list"]
                    for row in range(2, 24):
                        ws_list.cell(row=row, column=2, value=existing_list.cell(row=row, column=2).value)
                        ws_list.cell(row=row, column=3, value=existing_list.cell(row=row, column=3).value)
                existing_wb.close()
            break
        except (PermissionError, OSError) as e:
            last_error = e
            if attempt < max_retries - 1:
                _close_workbook_in_excel(output_path)
                time.sleep(retry_delay)
            continue
        except Exception:
            pass
            break

    for attempt in range(max_retries):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, message=".*Data Validation.*")
                wb.save(output_path)
            print("\nUpdated prediction list\n" if file_existed else "\nPrediction list created\n")
            return output_path
        except (PermissionError, OSError) as e:
            last_error = e
            if attempt < max_retries - 1:
                _close_workbook_in_excel(output_path)
                time.sleep(retry_delay)
            else:
                dir_name = os.path.dirname(output_path)
                base_name = os.path.basename(output_path)
                name_no_ext, ext = os.path.splitext(base_name)
                fallback_path = os.path.join(dir_name, f"{name_no_ext}_new{ext}")
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=UserWarning, message=".*Data Validation.*")
                        wb.save(fallback_path)
                    print(
                        "\nThe file was open in Excel, so the updated list was saved to:\n"
                        f"  {fallback_path}\n"
                        "Close the original file in Excel, then replace it with this file\n"
                        "(or rename this file to driver_pred_list.xlsx).\n"
                    )
                    return fallback_path
                except Exception:
                    print(
                        "\nCould not save: the file may be open in Excel or another program.\n"
                        "Please close driver_pred_list.xlsx and run the function again.\n"
                    )
                    raise


if __name__ == "__main__":
    create_driver_pred_list()
