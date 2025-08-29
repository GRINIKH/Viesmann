@echo off
cd /d "S:\PyScarper\Viesmann\main"
call "S:\PyScarper\Viesmann\.venv\Scripts\activate.bat"
python "S:\PyScarper\Viesmann\main\shop_Viessmann_BD.py"
python "S:\PyScarper\Viesmann\main\Viessmann_Parsing_V08.py"