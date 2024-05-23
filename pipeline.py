import os
import luigi
import requests
import tarfile
import gzip
import shutil
import pandas as pd
import io
import logging
import wget

# Инициализация логирования
logger = logging.getLogger('luigi-interface')
logging.basicConfig(level=logging.INFO)

## Шаг 1: Загрузка архива с данными
class DownloadDataset(luigi.Task):
    # Параметры задачи
    data_dir = luigi.Parameter(default='data')
    dataset_series = luigi.Parameter(default='GSE68nnn')
    dataset_name = luigi.Parameter(default='GSE68849')

    def output(self):
        # Указываем путь к выходному файлу (архиву)
        return luigi.LocalTarget(os.path.join(self.data_dir, f"{self.dataset_name}_RAW.tar"))

    def run(self):
        # Создаем директорию для скачивания, если она не существует
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Формируем URL для скачивания архива
        download_url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_series}/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        logger.info(f"URL для скачивания: {download_url}")
        
        # Скачиваем архив
        try:
            output_path = self.output().path
            wget.download(download_url, out=output_path)
            logger.info(f"Файл скачан по пути:{output_path}")
        except Exception as e:
            logger.error(f"Ошибка при скачивании файла: {e}")
            raise

    def complete(self):
        # Проверяем, что файл был успешно скачан и не пустой
        file_path = self.output().path
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > 0:
                logger.info(f'Размер скачанного датасета: {file_size} байт')
                return True
        logger.info("Скачанный файл отсутствует или пустой")
        return False

## Шаг 2: Разархивирование и извлечение файлов
class ExtractFiles(luigi.Task):
    # Параметры для задания
    data_dir = luigi.Parameter(default='data')
    dataset_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return DownloadDataset(data_dir=self.data_dir, dataset_series='GSE68nnn', dataset_name=self.dataset_name)

    def output(self):
        # Указываем путь к директории с извлеченными файлами
        return luigi.LocalTarget(os.path.join(self.data_dir, self.dataset_name, 'extracted'))

    def run(self):
        # Путь к tar-архиву
        tar_path = self.input().path
        # Путь для извлечения файлов
        extract_path = self.output().path
        os.makedirs(extract_path, exist_ok=True)

        # Распаковка tar архива
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(path=extract_path)
            logger.info(f"Распакован tar-архив в: {extract_path}")

        # Распаковка всех gzip файлов и создание под каждую папку
        for root, _, files in os.walk(extract_path):
            for file in files:
                if file.endswith('.gz'):
                    gzip_file_path = os.path.join(root, file)
                    file_name_without_ext = os.path.splitext(os.path.splitext(file)[0])[0]
                    output_dir = os.path.join(root, file_name_without_ext)
                    os.makedirs(output_dir, exist_ok=True)
                    output_file_path = os.path.join(output_dir, file_name_without_ext + '.txt')

                    # Распаковка gzip файла
                    with gzip.open(gzip_file_path, 'rb') as f_in:
                        with open(output_file_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    os.remove(gzip_file_path)
                    logger.info(f"Распакован gzip-файл в: {output_file_path}")

        # Проверка количества и названий файлов
        extracted_files = []
        for root, _, files in os.walk(extract_path):
            for file in files:
                extracted_files.append(os.path.join(root, file))
        logger.info(f'Извлеченные файлы: {extracted_files}')
        logger.info(f'Количество извлеченных файлов: {len(extracted_files)}')

## Шаг 3: Обработка текстовых файлов и извлечение данных
class ProcessTextFiles(luigi.Task):
    # Параметры для задания
    data_dir = luigi.Parameter(default='data')
    dataset_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return ExtractFiles(data_dir=self.data_dir, dataset_name=self.dataset_name)

    def output(self):
        # Указываем путь к директории с обработанными файлами
        return luigi.LocalTarget(os.path.join(self.data_dir, self.dataset_name, 'processed'))

    def run(self):
        # Путь к извлеченным файлам
        extract_path = os.path.join(self.data_dir, self.dataset_name, 'extracted')
        # Путь для сохранения обработанных файлов
        processed_path = self.output().path
        os.makedirs(processed_path, exist_ok=True)

        # Обработка каждого текстового файла
        for root, _, files in os.walk(extract_path):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    self.process_file(file_path, processed_path)

    def process_file(self, file_path, output_dir):
        dfs = {}
        with open(file_path, 'r') as f:
            write_key = None
            fio = io.StringIO()
            for line in f:
                 # Начало новой таблицы, заголовок которой начинается с символа '['
                if line.startswith('['):
                    if write_key:
                        # Сохранение текущей таблицы в словарь
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                    fio = io.StringIO()
                    write_key = line.strip('[]\n')
                    continue
                if write_key:
                    fio.write(line)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Сохранение каждой таблицы в отдельный TSV файл
        for key, df in dfs.items():
            output_file = os.path.join(output_dir, f"{key}.tsv")
            df.to_csv(output_file, sep='\t', index=False)

        # Обработка таблицы Probes, если она существует
        if 'Probes' in dfs:
            self.process_probes(dfs['Probes'], output_dir)

    def process_probes(self, probes_df, output_dir):

        # Урезанная версия таблицы Probes
        columns_to_drop = ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence']
        reduced_probes_df = probes_df.drop(columns=columns_to_drop)
        reduced_probes_path = os.path.join(output_dir, 'Probes_reduced.tsv')
        reduced_probes_df.to_csv(reduced_probes_path, sep='\t', index=False)
        logger.info(f"Урезанная версия таблицы Probes сохранена в: {reduced_probes_path}")

# Шаг 4: Удаление исходных текстовых файлов и создание readme.txt
class CleanupFiles(luigi.Task):
    # Параметры для задания
    data_dir = luigi.Parameter(default='data')
    dataset_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        # Эта задача зависит от задачи ProcessTextFiles
        return ProcessTextFiles(data_dir=self.data_dir, dataset_name=self.dataset_name)

    def output(self):
        # Указываем путь к файлу readme.txt
        return luigi.LocalTarget(os.path.join(self.data_dir, self.dataset_name, 'readme.txt'))

    def run(self):
        # Путь к извлеченным файлам
        extract_path = os.path.join(self.data_dir, self.dataset_name, 'extracted')
        removed_files = []

        # Удаление всех исходных текстовых файлов
        for root, _, files in os.walk(extract_path):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    removed_files.append(file_path)
                    logger.info(f"Удален исходный текстовый файл: {file_path}")

       # Создание readme.txt с информацией о выполненных шагах и удаленных файлах
        readme_path = os.path.join(self.data_dir, self.dataset_name, 'readme.txt')
        with open(readme_path, 'w') as readme_file:
            readme_file.write("Пайплайн выполнен успешно.\n\n")
            readme_file.write("### Шаги выполнения ###\n")
            readme_file.write("1. Загрузка архива с данными\n")
            readme_file.write("2. Распаковка tar-архива и gzip-файлов\n")
            readme_file.write("3. Обработка текстовых файлов и сохранение их в формате TSV\n")
            readme_file.write("4. Удаление исходных текстовых файлов\n\n")
            
            readme_file.write("### Удаленные файлы ###\n")
            for file_path in removed_files:
                readme_file.write(file_path + '\n')
            
            logger.info(f"Файл readme.txt создан по пути: {readme_path}")

# Выполнение всех шагов

class RunAllTasks(luigi.WrapperTask):
    data_dir = luigi.Parameter(default='data')
    dataset_series = luigi.Parameter(default='GSE68nnn')
    dataset_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return CleanupFiles(data_dir=self.data_dir, dataset_name=self.dataset_name)

if __name__ == "__main__":
    luigi.run()
