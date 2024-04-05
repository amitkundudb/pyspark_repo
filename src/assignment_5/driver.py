from src.assignment_5.util import *

if __name__ == "__main__":
    employee_df, department_df, country_df = create_dataframes()

    avg_salary_by_department(employee_df)

    employees_starts_with_m(employee_df, department_df)

    add_bonus_column(employee_df)

    reorder_columns(employee_df)

    perform_joins(employee_df, department_df)

    update_state_to_country_name(employee_df, country_df)

    lowercase_columns_and_add_load_date(employee_df)