# Covid-19 Spark DataFrame API

Задание выполнено на Apache Spark с использованием DataFrame API.

## Запуск

```bash
./run_covid_spark.sh
```

Скрипт читает `covid-data.csv`, печатает результаты в консоль и создает итоговые CSV-файлы:

- `top_15_cases_percent.csv`
- `top_10_new_cases.csv`
- `russia_cases_delta.csv`

Файл `covid-data.csv` должен лежать рядом со скриптом перед запуском. В репозиторий он не добавлен, потому что для проверки задания нужны скрипт и результаты выборок.

При запуске Spark также локально создает служебные папки:

- `result_top_15_cases_percent`
- `result_top_10_new_cases`
- `result_russia_cases_delta`

Эти папки не добавлены в репозиторий, потому что в GitHub уже лежат готовые CSV-файлы с результатами.

## Результаты

### 15 стран с наибольшим процентом заболевших на 2021-03-31

| iso_code | country | cases_percent |
|---|---|---:|
| AND | Andorra | 15.54 |
| MNE | Montenegro | 14.52 |
| CZE | Czechia | 14.31 |
| SMR | San Marino | 13.94 |
| SVN | Slovenia | 10.37 |
| LUX | Luxembourg | 9.85 |
| ISR | Israel | 9.63 |
| USA | United States | 9.20 |
| SRB | Serbia | 8.83 |
| BHR | Bahrain | 8.49 |
| PAN | Panama | 8.23 |
| PRT | Portugal | 8.06 |
| EST | Estonia | 8.02 |
| SWE | Sweden | 7.97 |
| LTU | Lithuania | 7.94 |

### Top 10 стран по максимальному числу новых случаев за 2021-03-25 - 2021-03-31

| date | country | new_cases |
|---|---|---:|
| 2021-03-25 | Brazil | 100158 |
| 2021-03-26 | United States | 77321 |
| 2021-03-31 | India | 72330 |
| 2021-03-31 | France | 59054 |
| 2021-03-31 | Turkey | 39302 |
| 2021-03-26 | Poland | 35145 |
| 2021-03-31 | Germany | 25014 |
| 2021-03-26 | Italy | 24076 |
| 2021-03-25 | Peru | 19206 |
| 2021-03-26 | Ukraine | 18226 |

### Изменение новых случаев в России за 2021-03-25 - 2021-03-31

| date | new_cases_yesterday | new_cases_today | delta |
|---|---:|---:|---:|
| 2021-03-25 | 8769 | 9128 | 359 |
| 2021-03-26 | 9128 | 9073 | -55 |
| 2021-03-27 | 9073 | 8783 | -290 |
| 2021-03-28 | 8783 | 8979 | 196 |
| 2021-03-29 | 8979 | 8589 | -390 |
| 2021-03-30 | 8589 | 8162 | -427 |
| 2021-03-31 | 8162 | 8156 | -6 |
