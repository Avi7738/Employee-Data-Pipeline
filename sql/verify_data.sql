-- 1. Total clean records loaded
SELECT COUNT(*) AS total_clean FROM employees_clean;

-- 2. Total rejected records
SELECT COUNT(*) AS total_rejected FROM employees_rejected;

-- 3. Rejection breakdown
SELECT reject_reason, COUNT(*) AS cnt
FROM employees_rejected
GROUP BY reject_reason
ORDER BY cnt DESC;

-- 4. Salary band distribution
SELECT * FROM salary_band_distribution;

-- 5. Department headcount + avg salary
SELECT * FROM dept_salary_summary;

-- 6. Check no future hire dates slipped through
SELECT COUNT(*) AS future_hires
FROM employees_clean
WHERE hire_date > CURRENT_DATE;

-- 7. Check no duplicate employee_ids
SELECT employee_id, COUNT(*) AS cnt
FROM employees_clean
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- 8. Sample clean rows
SELECT employee_id, full_name, email, email_domain,
       salary, salary_band, age, tenure_years, department, status
FROM employees_clean
LIMIT 10;

-- 9. Age sanity check (should be between 18 and 80)
SELECT MIN(age) AS min_age, MAX(age) AS max_age, AVG(age)::INTEGER AS avg_age
FROM employees_clean
WHERE age IS NOT NULL;

-- 10. Tenure sanity check
SELECT MIN(tenure_years), MAX(tenure_years), AVG(tenure_years)::DECIMAL(3,1)
FROM employees_clean;
