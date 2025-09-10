const API_URL = "https://ams-04i2.onrender.com/attendance/scan";

/**
 * Triggered whenever a cell is edited in Column A (badge_no).
 */
function onEdit(e) {
  try {
    if (!e || !e.range) return;

    const sheet = e.range.getSheet();
    const row = e.range.getRow();
    const col = e.range.getColumn();

    // Only trigger on Column A (skip header)
    if (col !== 1 || row === 1) return;

    const badgeNo = e.range.getValue().toString().trim();
    if (!badgeNo || badgeNo.startsWith("❌") || badgeNo.startsWith("⚠️")) return;

    const attendanceType = sheet.getRange("I1").getValue().toString().trim();
    const mode = sheet.getRange("H1").getValue().toString().trim();

    if (!attendanceType) {
      sheet.getRange(row, 2).setValue("⚠️ Set Attendance Type in I1");
      return;
    }
    if (!mode) {
      sheet.getRange(row, 2).setValue("⚠️ Set Mode in H1");
      return;
    }

    const timeStr = new Date().toLocaleTimeString("en-GB", { hour12: false });

    // Build payload for API
    const payload = {
      badge_no: badgeNo,
      attendance_type: attendanceType,
      remarks: null
    };
    if (mode === "Check-In") payload.check_in_time = timeStr;
    if (mode === "Check-Out") payload.check_out_time = timeStr;

    Logger.log("Sending payload: " + JSON.stringify(payload));

    // Call API
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
      let targetRow = row; // default to current scanned row

      // Handle Check-Out: update existing check-in row
      if (mode === "Check-Out") {
        const lastRow = sheet.getLastRow();
        let foundRow = null;

        for (let r = 2; r <= lastRow; r++) {
          const val = sheet.getRange(r, 1).getValue().toString().trim();
          if (val === badgeNo) {
            foundRow = r;
            break;
          }
        }

        if (!foundRow) {
          // No check-in exists → do not create row, show warning
          sheet.getRange(row, 2).setValue("❌ No Check-In found");
          return;
        }

        targetRow = foundRow;

        // Clear badge_no from the current scan row only
        if (row !== targetRow) {
          sheet.getRange(row, 1).setValue("");
        }
      }

      // Fill Name, Department, Timestamp
      sheet.getRange(targetRow, 2).setValue(result.name || "Recorded");
      sheet.getRange(targetRow, 3).setValue(result.department_name || "");
      sheet.getRange(targetRow, 4).setValue(new Date());

      // Fill Check-In / Check-Out
      if (result.check_in_time) sheet.getRange(targetRow, 5).setValue(result.check_in_time);
      if (result.check_out_time) sheet.getRange(targetRow, 6).setValue(result.check_o_
