# luigi-pipeline
Этот проект представляет собой пайплайн для обработки датасета GEO (Gene Expression Omnibus) с использованием фреймворка Luigi. Пайплайн включает в себя следующие шаги:

1. Скачивание архива с данными.
2. Распаковка tar-архива и gzip-файлов.
3. Обработка текстовых файлов и сохранение их в формате TSV.
4. Удаление исходных текстовых файлов и создание файла `readme.txt` с информацией о выполненных шагах.

## Структура проекта
- [pipeline.py](https://github.com/L-Gaysina/luigi-pipeline/blob/main/pipeline.py): Основной файл с кодом пайплайна Luigi.
- [requirements.txt](https://github.com/L-Gaysina/luigi-pipeline/blob/main/requirements.txt): Файл с зависимостями, необходимыми для выполнения пайплайна.
- [папка с датасетом](https://drive.google.com/drive/folders/1Cd430AMsyauHSPOnKTbnVI9N4XYvCnaO?usp=sharing), полученным в результате работы пайплайна

## Требования

Для запуска пайплайна необходим Python 3 и установленные зависимости из `requirements.txt`.

## Инструкция по установке и запуску

### 1. Установите зависимости:

```bash
pip install -r requirements.txt
```    

### 2. Для запуска всех задач пайплайна выполните следующую команду:

```bash
python pipeline.py RunAllTasks --local-scheduler --data-dir 'data' --dataset-series 'GSE68nnn' --dataset-name 'GSE68849'
```
