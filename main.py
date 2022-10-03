import sys

from database_util import DatabaseUtil

def main():
    try:
        arguments = sys.argv
        if not len(arguments) > 1:
            print("Supply an argument with what to do:")
            print("setup - Create tables")
            print("insert - Insert data to tables")
            print("tasks - Run tasks")
            return
        
        program = DatabaseUtil()
        todo = sys.argv[1]

        if todo == "setup":
            program.setup()

        if todo == "insert":
            program.insert_users()
            program.insert_trajectory()
        
        if todo == "tasks":
            program.task_2_1_count_tables_sizes()
            program.task_2_2_average_activities_per_user()
            program.task_2_3_top_20_users_with_most_activities()
            program.task_2_4_users_whos_taken_taxi()
            program.task_2_5_count_transportations()
            program.task_2_6_a_year_with_most_activities()
            program.task_2_6_b_year_with_longest_activities()
            program.task_2_7_total_distance_walked_in_2008_by_112()
            program.task_2_8_top_20_users_with_most_altitude_meters()
            program.task_2_9_invalid_activities_per_user()
            program.task_2_10_users_tracked_forbidden_city()
            program.task_2_11_users_most_used_transportation_mode()

        if program:
            program.connection.close_connection()
    except Exception as e:
        # program.cursor.close()
        # program.drop_all()  
        raise e


main()