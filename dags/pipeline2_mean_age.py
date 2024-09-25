
def mean_age_by_occupation():
    import pandas as pd
    from pipeline2_df import read_data_in_df

    user_df,rating_df,item_data=read_data_in_df()

    # Grouping by 'occupation' and calculating the mean age
    mean_age_by_occupation = user_df.groupby('occupation')['age'].mean().reset_index()
    print(mean_age_by_occupation)

#mean_age_by_occupation()