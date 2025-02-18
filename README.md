# Домашняя работа №14
# Memcache Loader

## Описание
Скрипт для загрузки данных о приложениях в Memcached

## Установка
```bash
pip install -r requirements.txt
```

## Запуск
```bash
cd homework
python memc_load.py --pattern "*.tsv.gz" --idfa "127.0.0.1:33013" --gaid "127.0.0.1:33014" --adid "127.0.0.1:33015" --dvid "127.0.0.1:33016"
```

## Тестирование
```bash
python -m unittest discover
```
