
from database_util import DatabaseUtil



def main():
    program = DatabaseUtil()
    try:
        #program.setup()
        #program.insert_users()
        #program.insert_trajectory()
        # program.task_2_1_count_tables_sizes()
        # program.task_2_2_average_activities_per_user()
        #program.task_2_3_top_20_users_with_most_activities()
        program.task_2_4_users_whos_taken_taxi()
        if program:
            program.connection.close_connection()
    except Exception as e:
        # program.cursor.close()
        # program.drop_all()  
        raise e


main()