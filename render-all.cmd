@echo off
REM Render all Quarto decks to Reveal.js (Windows cmd)

SETLOCAL ENABLEDELAYEDEXPANSION

IF "%~1"=="" (
  SET Q=quarto
) ELSE (
  SET Q=%~1
)

REM Root dirs
SET ROOT=%~dp0
CD /D "%ROOT%"

REM Ensure output dir exists
IF NOT EXIST _site mkdir _site

REM VBA decks
%Q% render slides\vba\01-intro-vba.qmd --to revealjs || goto :error
%Q% render slides\vba\02-vba-platforms-macros.qmd --to revealjs || goto :error
%Q% render slides\vba\03-vba-programming-basics.qmd --to revealjs || goto :error

REM Python decks
%Q% render slides\python\01-setup-env.qmd --to revealjs || goto :error
%Q% render slides\python\02-python-basics.qmd --to revealjs || goto :error
%Q% render slides\python\03-oop-types.qmd --to revealjs || goto :error
%Q% render slides\python\04-numpy-pandas.qmd --to revealjs || goto :error
%Q% render slides\python\05-optimization.qmd --to revealjs || goto :error
%Q% render slides\python\06-web-apps.qmd --to revealjs || goto :error
%Q% render slides\python\07-bigdata.qmd --to revealjs || goto :error

REM Copy production landing into _site
IF EXIST site-index.html copy /Y site-index.html _site\index.html >NUL

ECHO.
ECHO Render complete. Open _site\index.html
REM Auto-open the landing page for convenience
START "" _site\index.html
GOTO :eof

:error
ECHO.
ECHO Render failed. Ensure Quarto is installed and on PATH, or pass its path as first arg.
ECHO Example: render-all.cmd "C:\\Program Files\\Quarto\\bin\\quarto.exe"
EXIT /B 1
