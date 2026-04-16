from extract import extract_shows
from transform import *

def main():
    data = extract_shows()

    if not data:
        print("No data extracted")
        return
        
    df = create_dataframe(data)
    df = clean_runtime(df)

    type_dist = get_type_distribution(df)
    status = get_status_distribution(df)
    genres = get_genres_distribution(df)
    avg = get_avg_runtime_by_type(df)


    print("Type distribution : ")
    print(type_dist)

    print("Status : ")
    print(status)

    print("Genres : ")
    print(genres)

    print("Average run time : ")
    print(avg)

if __name__ == "__main__":
    main()
