// ---------------- CONFIG ----------------
const API_URL = "https://ams-04i2.onrender.com/attendance/scan";
const REPORT_URL_BASE = "https://ams-04i2.onrender.com/attendance/report";

// ---------------- AUTHORIZE ----------------
function authorize() {
  UrlFetchApp.fetch("https://www.google.com"); // Dummy call to request permission
}

// ---------------- ATTENDANCE SCAN ----------------
function handleEdit(e) {
  try {
    if (!e || !e.range) return;

    const sheet = e.range.getSheet();
    const row = e.range.getRow();
    const col = e.range.getColumn();
    const ui = SpreadsheetApp.getUi();

    // Only trigger on Column A (skip header)
    if (col !== 1 || row === 1) return;

    const badgeNo = e.range.getValue().toString().trim();
    if (!badgeNo || badgeNo.startsWith("❌") || badgeNo.startsWith("⚠️")) return;

    const controlSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Controls");
    if (!controlSheet) {
      ui.alert("⚠️ Controls sheet not found. Please run 'Update Attendance Type Dropdown'.");
      sheet.getRange(row, 1).setValue("");
      return;
    }

    const attendanceType = controlSheet.getRange("B1").getValue().toString().trim();
    const mode = controlSheet.getRange("B2").getValue().toString().trim(); // Check-In / Check-Out

    if (!attendanceType) {
      ui.alert("⚠️ Please select Attendance Type in Controls!B1.");
      sheet.getRange(row, 1).setValue(""); 
      return;
    }
    if (!mode) {
      ui.alert("⚠️ Please select Mode (Check-In / Check-Out) in Controls!C1.");
      sheet.getRange(row, 1).setValue(""); 
      return;
    }

    const timeStr = new Date().toLocaleTimeString("en-GB", { hour12: false });
    const payload = { badge_no: badgeNo, attendance_type: attendanceType, remarks: null };
    if (mode === "Check-In") payload.check_in_time = timeStr;
    if (mode === "Check-Out") payload.check_out_time = timeStr;

    Logger.log("Sending payload: " + JSON.stringify(payload));

    const response = UrlFetchApp.fetch(API_URL, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    const status = response.getResponseCode();
    const result = JSON.parse(response.getContentText() || "{}");
    Logger.log("API response: " + JSON.stringify(result));

    if (status === 200) {
      let targetRow = row;

      // For Check-Out, locate existing check-in row
      if (mode === "Check-Out") {
        const lastRow = sheet.getLastRow();
        let foundRow = null;
        for (let r = 2; r <= lastRow; r++) {
          const val = sheet.getRange(r, 1).getValue().toString().trim();
          const typeCell = sheet.getRange(r, 7).getValue().toString().trim(); // Column G stores attendance_type
          if (val === badgeNo && typeCell === attendanceType) {
            foundRow = r;
            break;
          }
        }

        if (!foundRow) {
          ui.alert("❌ No Check-In found for this badge today for this event.");
          sheet.getRange(row, 1).setValue("");
          return;
        }

        targetRow = foundRow;
        sheet.getRange(row, 1).setValue(""); // clear scanned badge row
      }

      // Fill Name, Department, Timestamp
      sheet.getRange(targetRow, 2).setValue(result.name || "Recorded");
      sheet.getRange(targetRow, 3).setValue(result.department_name || "");
      sheet.getRange(targetRow, 4).setValue(new Date());

      // Fill Check-In / Check-Out
      if (result.check_in_time) sheet.getRange(targetRow, 5).setValue(result.check_in_time);
      if (result.check_out_time) sheet.getRange(targetRow, 6).setValue(result.check_out_time);

      // Store attendance_type in Column G for smart clearing
      sheet.getRange(targetRow, 7).setValue(attendanceType);

      // Highlight duplicates for Check-In
      if (mode === "Check-In") {
        for (let r = 2; r < targetRow; r++) {
          const val = sheet.getRange(r, 1).getValue().toString().trim();
          const typeCell = sheet.getRange(r, 7).getValue().toString().trim();
          if (val === badgeNo && typeCell === attendanceType) {
            sheet.getRange(targetRow, 1, 1, 7).setBackground("#ff9999");
            sheet.getRange(r, 1, 1, 7).setBackground("#ff9999");
            break;
          }
        }
      }

      ui.alert("✅ Attendance recorded for: " + result.name);

    } else {
      ui.alert("❌ Failed: " + (result.detail || "Unknown error"));
      sheet.getRange(row, 1).setValue(""); 
    }

  } catch (err) {
    Logger.log("handleEdit error: " + err);
    SpreadsheetApp.getUi().alert("❌ Error: " + err);
  }
}

// ---------------- REPORTING ----------------
function fetchAttendanceReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const controlSheet = ss.getSheetByName("Controls");
  const ui = SpreadsheetApp.getUi();

  if (!controlSheet) {
    ui.alert("⚠️ Please run 'Update Attendance Type Dropdown' first.");
    return;
  }

  const attendanceType = controlSheet.getRange("B1").getValue().toString().trim();
  if (!attendanceType) {
    ui.alert("⚠️ Please select an attendance type in Controls!B1.");
    return;
  }

  const url = `${REPORT_URL_BASE}?attendance_type=${encodeURIComponent(attendanceType)}`;
  const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });

  if (response.getResponseCode() !== 200) {
    ui.alert("❌ Failed to fetch report");
    return;
  }

  const data = JSON.parse(response.getContentText());
  const report = data.report;

  // --- Group by department ---
  const grouped = {};
  report.forEach(r => {
    if (!grouped[r.department]) grouped[r.department] = [];
    grouped[r.department].push(r);
  });

  // --- Create sheet with date ---
  const dateStr = new Date().toISOString().split("T")[0];
  const sheetName = `Report_${dateStr}`;
  const existing = ss.getSheetByName(sheetName);
  if (existing) ss.deleteSheet(existing);
  const sheet = ss.insertSheet(sheetName);

  let row = 1;
  sheet.getRange(row, 1).setValue(`Attendance Report: ${data.date} (${data.attendance_type})`);
  sheet.getRange(row, 1).setFontWeight("bold").setFontSize(14);
  row += 2;

  Object.keys(grouped).sort().forEach(dept => {
    sheet.getRange(row, 1).setValue(`Department: ${dept}`);
    sheet.getRange(row, 1).setFontWeight("bold").setFontSize(12);
    row++;

    sheet.getRange(row, 1, 1, 4).setValues([["Name", "Badge No", "Department", "Status"]]);
    sheet.getRange(row, 1, 1, 4).setFontWeight("bold");
    row++;

    const present = grouped[dept].filter(r => r.status === "Present");
    const absent = grouped[dept].filter(r => r.status === "Absent");

    present.forEach(r => {
      sheet.getRange(row, 1, 1, 4).setValues([[r.name, r.badge_no, r.department, r.status]]);
      sheet.getRange(row, 1, 1, 4).setBackground("#d9ead3"); // light green
      row++;
    });

    if (present.length && absent.length) row++;

    absent.forEach(r => {
      sheet.getRange(row, 1, 1, 4).setValues([[r.name, r.badge_no, r.department, r.status]]);
      sheet.getRange(row, 1, 1, 4).setBackground("#f4cccc"); // light red
      row++;
    });

    row += 2;
  });

  sheet.autoResizeColumns(1, 4);
  ui.alert(`✅ Grouped & color-coded report created: ${sheetName}`);
}

// ---------------- CONTROL SHEET SETUP ----------------
function setupAttendanceTypeDropdown() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Controls") || ss.insertSheet("Controls");
  sheet.clear();

  const types = [
    "Sunday_Satsang", "Wednesday_Satsang",
    "WeekDay", "WeekNight",
    "Bhati", "Beas", "Others"
  ];

  // Attendance Type
  sheet.getRange("A1").setValue("Select Attendance Type:");
  const typeCell = sheet.getRange("B1");
  typeCell.setDataValidation(
    SpreadsheetApp.newDataValidation().requireValueInList(types, true).build()
  );
  typeCell.setValue(types[0]);

  // Mode
  sheet.getRange("A2").setValue("Select Mode (Check-In / Check-Out):");
  const modeCell = sheet.getRange("B2");  // <-- change to B2 for alignment
  modeCell.setDataValidation(
    SpreadsheetApp.newDataValidation().requireValueInList(["Check-In","Check-Out"], true).build()
  );
  modeCell.setValue("Check-In");

  SpreadsheetApp.getUi().alert("✅ Controls sheet setup complete.");
}


// ---------------- CLEAR PREVIOUS EVENT SCANS ----------------
function clearPreviousEventScans() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Attendance_Scan");
  const controlSheet = ss.getSheetByName("Controls");
  const ui = SpreadsheetApp.getUi();

  if (!sheet) { ui.alert("⚠️ Attendance_Scan sheet not found."); return; }
  if (!controlSheet) { ui.alert("⚠️ Controls sheet not found."); return; }

  const currentType = controlSheet.getRange("B1").getValue().toString().trim();
  if (!currentType) { ui.alert("⚠️ Please select an attendance type in Controls!B1."); return; }

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) { ui.alert("Sheet is already empty."); return; }

  for (let r = lastRow; r >= 2; r--) {
    const typeCell = sheet.getRange(r, 7).getValue().toString().trim(); // Column G stores attendance_type
    if (typeCell !== currentType) {
      sheet.getRange(r, 1, 1, sheet.getLastColumn()).clearContent();
    }
  }

  ui.alert(`✅ Previous event scans cleared. Current event (${currentType}) preserved.`);
}

// ---------------- MENU ----------------
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("Attendance")
    .addItem("Update Attendance Type Dropdown", "setupAttendanceTypeDropdown")
    .addItem("Fetch Attendance Report", "fetchAttendanceReport")
    .addItem("Clear Previous Event Scans", "clearPreviousEventScans")
    .addToUi();
}
