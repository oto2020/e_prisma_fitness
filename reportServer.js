const express = require('express');
const cors = require('cors');
const { PrismaClient } = require('@prisma/client');

const app = express();
const prisma = new PrismaClient();

app.use(express.json());
app.use(cors());

app.post('/trainers_conversation_for_month', async (req, res) => {
    try {
        const {
            year,
            month,
            serviceName,
            divisions,
            conversationDays
        } = req.body;

        if (!year || !month || !serviceName || !divisions.length || !conversationDays) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        const placeholders = divisions.map(() => '?').join(', '); // Формируем ?, ?, ?, ... в зависимости от длины массива

        const query = `
            WITH ServiceCounts AS (
                SELECT 
                    s.trainer,
                    COUNT(*) AS sessions_count_vpt
                FROM services s
                WHERE YEAR(s.datetime) = ?
                  AND MONTH(s.datetime) = ?
                  AND s.name = ?
                GROUP BY s.trainer
            ),
            FirstSales AS (
                SELECT 
                    s.trainer,
                    s.client,
                    MIN(sa.datetime) AS first_sale_date,
                    MIN(sa.id) AS first_sale_id
                FROM services s
                JOIN sales sa 
                    ON s.client = sa.client 
                    AND s.trainer = sa.trainer
                    AND sa.division IN (${placeholders}) 
                    AND sa.datetime >= s.datetime
                    AND sa.datetime <= DATE_ADD(s.datetime, INTERVAL ? DAY)
                    AND sa.final_price > 0
                WHERE YEAR(s.datetime) = ?
                  AND MONTH(s.datetime) = ?
                  AND s.name = ?
                GROUP BY s.trainer, s.client
            ),
            FirstSalesSummary AS (
                SELECT 
                    fs.trainer,
                    COUNT(fs.client) AS first_sales_count_vpt,
                    COALESCE(SUM(sa.final_price), 0) AS total_first_sales_amount_vpt
                FROM FirstSales fs
                JOIN sales sa ON fs.first_sale_id = sa.id
                WHERE sa.final_price > 0
                GROUP BY fs.trainer
            )
            SELECT 
                COALESCE(sc.trainer, fs.trainer) AS trainer,
                COALESCE(sc.sessions_count_vpt, 0) AS sessions_count_vpt,
                COALESCE(fs.first_sales_count_vpt, 0) AS first_sales_count_vpt,
                COALESCE(fs.total_first_sales_amount_vpt, 0) AS total_first_sales_amount_vpt
            FROM ServiceCounts sc
            LEFT JOIN FirstSalesSummary fs ON sc.trainer = fs.trainer

            UNION

            SELECT 
                COALESCE(sc.trainer, fs.trainer) AS trainer,
                COALESCE(sc.sessions_count_vpt, 0) AS sessions_count_vpt,
                COALESCE(fs.first_sales_count_vpt, 0) AS first_sales_count_vpt,
                COALESCE(fs.total_first_sales_amount_vpt, 0) AS total_first_sales_amount_vpt
            FROM FirstSalesSummary fs
            LEFT JOIN ServiceCounts sc ON sc.trainer = fs.trainer

            ORDER BY trainer;
        `;

        const params = [
            year, 
            month, 
            serviceName, 
            ...divisions, // Передаём все значения из divisions
            conversationDays, 
            year, 
            month, 
            serviceName
        ];

        const result = await prisma.$queryRawUnsafe(query, ...params);

        const serializedResult = result.map(row =>
            Object.fromEntries(
                Object.entries(row).map(([key, value]) => [
                    key,
                    typeof value === "bigint" ? value.toString() : value
                ])
            )
        );
        
        res.json(serializedResult);
        
    } catch (error) {
        console.error("Ошибка выполнения запроса:", error);
        res.status(500).json({ error: "Ошибка выполнения запроса" });
    } finally {
        await prisma.$disconnect();
    }
});

app.post('/trainers_sales_for_month', async (req, res) => {
    try {
        const { year, month, divisions } = req.body;

        if (!year || !month || !divisions.length) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        const placeholders = divisions.map(() => '?').join(', ');
        
        const query = `
            WITH FirstSales AS (
                SELECT s.client, s.trainer, MIN(s.datetime) AS first_purchase_date
                FROM sales s
                GROUP BY s.client, s.trainer
            )
            SELECT 
                s.trainer,
                COUNT(s.id) AS total_sales_count,
                COALESCE(SUM(s.final_price), 0) AS total_sales_amount,
                
                COUNT(CASE WHEN fs.first_purchase_date = s.datetime THEN 1 END) AS first_sales_count,
                COALESCE(SUM(CASE WHEN fs.first_purchase_date = s.datetime THEN s.final_price END), 0) AS first_sales_amount,
                
                COUNT(CASE WHEN fs.first_purchase_date < s.datetime THEN 1 END) AS renewal_sales_count,
                COALESCE(SUM(CASE WHEN fs.first_purchase_date < s.datetime THEN s.final_price END), 0) AS renewal_sales_amount
            FROM sales s
            LEFT JOIN FirstSales fs ON s.client = fs.client AND s.trainer = fs.trainer
            WHERE YEAR(s.datetime) = ?
              AND MONTH(s.datetime) = ?
              AND s.division IN (${placeholders})
            GROUP BY s.trainer
            ORDER BY s.trainer;
        `;

        const params = [year, month, ...divisions];

        const result = await prisma.$queryRawUnsafe(query, ...params);

        const serializedResult = result.map(row =>
            Object.fromEntries(
                Object.entries(row).map(([key, value]) => [
                    key,
                    typeof value === "bigint" ? value.toString() : value
                ])
            )
        );

        res.json(serializedResult);

    } catch (error) {
        console.error("Ошибка выполнения запроса:", error);
        res.status(500).json({ error: "Ошибка выполнения запроса" });
    } finally {
        await prisma.$disconnect();
    }
});

app.post('/trainers_services_for_month', async (req, res) => {
    try {
        const { year, month, divisions } = req.body;

        if (!year || !month || !divisions.length) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        const placeholders = divisions.map(() => '?').join(', ');

        const query = `
            WITH FreeGroupServices AS (
                SELECT trainer, COUNT(*) AS free_group_count, COALESCE(SUM(price), 0) AS free_group_amount
                FROM services
                WHERE trainer IS NOT NULL AND trainer <> ''
                  AND name NOT LIKE '%МГ %'
                  AND name NOT LIKE '%ПТ %'
                  AND name NOT LIKE '%KIDS%'
                  AND name NOT LIKE '%₽%'
                  AND name NOT LIKE '%$%'
                  AND name NOT LIKE '%СПЛИТ%'
                  AND name NOT LIKE '%тестирование%'
                  AND name NOT LIKE '%браслет%'
                  AND name NOT LIKE '%соляри%'
                  AND name NOT LIKE '%питания%'
                  AND name NOT LIKE '%скриннинг%'
                  AND name NOT LIKE '%доп.%'
                  AND name NOT LIKE '%столик%'
                  AND name NOT LIKE '%пробковый%'
                  AND name NOT LIKE '%массаж%'
                  AND name NOT LIKE '%липопластика%'
                  AND name NOT LIKE '%подарочный%'
                  AND name NOT LIKE '%сертификат%'
                  AND BINARY name = BINARY UPPER(name)
                  AND YEAR(datetime) = ? AND MONTH(datetime) = ?
                  AND division IN (${placeholders})
                GROUP BY trainer
            ),
            GPServices AS (
                SELECT trainer, COUNT(*) AS gp_count, COALESCE(SUM(price), 0) AS gp_amount
                FROM services
                WHERE trainer IS NOT NULL AND trainer <> ''
                  AND name LIKE '% ₽' ESCAPE ''
                  AND YEAR(datetime) = ? AND MONTH(datetime) = ?
                  AND division IN (${placeholders})
                GROUP BY trainer
            )
            SELECT 
                s.trainer,
                COUNT(CASE WHEN s.name LIKE '%ПТ %' OR s.name LIKE '%СПЛИТ %' THEN 1 END) AS pt_count,
                COALESCE(SUM(CASE WHEN s.name LIKE '%ПТ %' OR s.name LIKE '%СПЛИТ %' THEN s.price END), 0) AS pt_amount,
                COUNT(CASE WHEN s.name LIKE '%МГ %' THEN 1 END) AS mg_count,
                COALESCE(SUM(CASE WHEN s.name LIKE '%МГ %' THEN s.price END), 0) AS mg_amount,
                COALESCE(fg.free_group_count, 0) AS free_group_count,
                COALESCE(fg.free_group_amount, 0) AS free_group_amount,
                COALESCE(gp.gp_count, 0) AS gp_count,
                COALESCE(gp.gp_amount, 0) AS gp_amount
            FROM services s
            LEFT JOIN FreeGroupServices fg ON s.trainer = fg.trainer
            LEFT JOIN GPServices gp ON s.trainer = gp.trainer
            WHERE YEAR(s.datetime) = ? AND MONTH(s.datetime) = ?
              AND s.division IN (${placeholders})
            GROUP BY s.trainer
            ORDER BY s.trainer;
        `;

        const params = [year, month, ...divisions, year, month, ...divisions, year, month, ...divisions];

        const result = await prisma.$queryRawUnsafe(query, ...params);

        const serializedResult = result.map(row =>
            Object.fromEntries(
                Object.entries(row).map(([key, value]) => [
                    key,
                    typeof value === 'bigint' ? value.toString() : value
                ])
            )
        );

        res.json(serializedResult);
    } catch (error) {
        console.error('Ошибка выполнения запроса:', error);
        res.status(500).json({ error: 'Ошибка выполнения запроса' });
    } finally {
        await prisma.$disconnect();
    }
});




const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
