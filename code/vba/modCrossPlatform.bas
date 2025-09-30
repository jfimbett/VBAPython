Attribute VB_Name = "modCrossPlatform"
Option Explicit

Public Function DesktopPath() As String
    If InStr(Application.OperatingSystem, "Mac") > 0 Then
        DesktopPath = Environ$("HOME") & "/Desktop/"
    Else
        DesktopPath = Environ$("USERPROFILE") & "\Desktop\"
    End If
End Function

Public Sub SaveActiveSheetCsv()
    Dim p As String: p = DesktopPath() & "sheet.csv"
    ActiveWorkbook.SaveAs Filename:=p, FileFormat:=xlCSVUTF8
End Sub
