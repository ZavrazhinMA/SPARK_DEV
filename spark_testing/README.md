В материалах к уроку приложен файл с данными о странах countries.json.
Пользуясь примером проекта Maven/Spark/Scala, разобранном на занятии, напишите следующие функции:

Функция, которая возвращает датафрейм со странами, которые граничат с 5 или более чем с 5 странами. В датафрейме должны быть столбцы:
Country - Международное название страны
NumBorders - Количество граничащих стран
BorderCountries - Список граничащих стран в формате строки через запятую.
Функция, которая возвращает рейтинг языков, на которых говорят в наибольшем количестве стран. В датафрейме должны быть столбцы:
Language - название языка;
NumCountries - количество стран, в которых говорят на языке;
Countries - список международных названий стран, в которых говорят на языке, в формате `ArrayType
К функциям придумайте напишите 2 технических и 1 функциональный тест (можно больше). Например: функции возвращают ожидаемую структуру датафрейма. В качестве функционального теста можно вручную создать датафрейм-эталон, затем сравнить его содержимое с тем, что вернула функция. Сравнение одной или нескольких строк достаточно. Например, можно проверить, что первая функция возвращает датафрейм, содержащий информацию о РФ.
Результат выложить в репозиторий git и прислать ссылку. Можно также прислать весь проект полностью в архиве.