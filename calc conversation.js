const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

// 🔹 Переменные для настройки
const targetServiceYear = 2024;  // Год
const targetServiceMonths = [10, 11, 12];  // Номера месяцев
const targetServiceName = 'Персональная тренировка в тренажерном зале';
const saleDivisions = ['Тренажерный зал'];
const conversationDays = 60; // Сколько дней после тренировки учитывать для первой продажи

async function fetchData() {
    try {
        // 🔹 Создаем строки для динамических параметров
        const monthsList = targetServiceMonths.map(m => `'${m}'`).join(',');
        const divisionsList = saleDivisions.map(d => `'${d}'`).join(',');

        // 🔹 Формируем SQL-запрос с динамическими параметрами
        const query = `
            WITH FilteredServices AS (
                SELECT 
                    trainer,
                    client,
                    MONTH(datetime) AS month,
                    datetime AS service_date,
                    COUNT(*) AS conducted_trainings
                FROM services
                WHERE YEAR(datetime) = ?
                  AND MONTH(datetime) IN (${monthsList})
                  AND name = ?
                GROUP BY trainer, client, month, service_date
            ),
            FirstSales AS (
                SELECT 
                    sale.trainer,
                    sale.client,
                    MONTH(sale.datetime) AS month,
                    MIN(sale.datetime) AS first_sale_date,
                    SUM(sale.final_price) AS sales_amount
                FROM sales sale
                JOIN FilteredServices fs ON sale.client = fs.client 
                                         AND sale.trainer = fs.trainer 
                                         AND sale.division IN (${divisionsList})
                                         AND sale.datetime BETWEEN fs.service_date AND DATE_ADD(fs.service_date, INTERVAL ${conversationDays} DAY)
                GROUP BY sale.trainer, sale.client, month
            )
            SELECT 
                fs.trainer,
                ${targetServiceMonths.map(m => `                
                SUM(CASE WHEN fs.month = ${m} THEN fs.conducted_trainings ELSE 0 END) AS 'проведено_${m}',
                COUNT(CASE WHEN fs.month = ${m} AND fs.client IN (SELECT client FROM FirstSales WHERE month = ${m}) THEN 1 END) AS 'продаж_${m}',
                FORMAT(SUM(CASE WHEN fs.month = ${m} THEN 
                    (SELECT sales_amount FROM FirstSales WHERE FirstSales.trainer = fs.trainer AND FirstSales.client = fs.client AND FirstSales.month = ${m}) 
                    ELSE 0 END), 0, 'ru_RU') AS 'сумма_${m}'
                `).join(',')}
            FROM FilteredServices fs
            GROUP BY fs.trainer
            ORDER BY fs.trainer;
        `;

        // 🔹 Выполняем запрос через Prisma ORM
        const result = await prisma.$queryRawUnsafe(query, targetServiceYear, targetServiceName);

        // 🔹 Динамический заголовок таблицы
        let header = "Тренер\t" + targetServiceMonths.map(m => 
            `Проведено ${targetServiceYear}-${m}\tПродаж ${targetServiceYear}-${m}\tСумма ${targetServiceYear}-${m}`
        ).join('\t');
        console.log(header);

        // 🔹 Выводим результат в консоль
        result.forEach(row => {
            let rowData = `${row.trainer}\t`;
            rowData += targetServiceMonths.map(m => 
                `${row[`проведено_${m}`] || 0}\t${row[`продаж_${m}`] || 0}\t${row[`сумма_${m}`] || '0 ₽'}`
            ).join('\t');
            console.log(rowData);
        });

    } catch (error) {
        console.error('Ошибка при выполнении запроса:', error);
    } finally {
        await prisma.$disconnect();
    }
}

// 🔹 Запускаем выполнение
fetchData();
