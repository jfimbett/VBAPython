Attribute VB_Name = "modFinanceBasics"
Option Explicit

Public Sub DailyReturn()
    ' Assumes close in column B starting at row 2
    Dim last As Long, i As Long
    last = Cells(Rows.Count, 2).End(xlUp).Row
    Range("C1").Value = "ret"
    For i = 3 To last
        If Cells(i - 1, 2).Value <> 0 Then
            Cells(i, 3).Value = Cells(i, 2).Value / Cells(i - 1, 2).Value - 1
        End If
    Next i
End Sub
