#%%
import pandas as pd
import numpy as np

# Load the Titanic dataset
df = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv')

# Explore the structure
print(df.head())
print(df.info())
print(df.describe())

#%%
# Check for missing values
print(df.isnull().sum())

# Basic statistics
print(f"Total passengers: {len(df)}")
print(f"Survival rate: {df['survived'].mean():.2%}")
print(f"\nPassengers per class:")
print(df['pclass'].value_counts().sort_index())
# %%
# Task 1: Calculate survival rates by passenger class
survival_by_class = df.groupby('pclass')['survived'].agg([
    ('count', 'count'),
    ('survived', 'sum'),
    ('survival_rate', 'mean')
])

print("Survival rates by class:")
print(survival_by_class)

# Visualization insight:
# 1st class: ~63% survival
# 2nd class: ~47% survival  
# 3rd class: ~24% survival
# Clear pattern: higher class = higher survival rate
# %%
# Task 2: Average age by gender and survival status
age_analysis = df.groupby(['sex', 'survived'])['age'].mean()
print("\nAverage age by gender and survival:")
print(age_analysis)

# More detailed view
age_detail = df.groupby(['sex', 'survived']).agg({
    'age': ['mean', 'median', 'std', 'count'],
    'fare': {'mean', 'median', 'std', 'count'}
})
print("\nDetailed age statistics:")
print(age_detail)

# Insight: Women and children first policy visible in data
# Younger passengers more likely to survive
# %%
# Task 3: Survival rate by deck (extracted from Cabin)
# First, extract deck letter from Cabin

# Calculate survival rates by deck
deck_survival = df.groupby('deck')['survived'].agg([
    'count', 'mean'
]).sort_values('mean', ascending=False)

print("\nSurvival rates by deck:")
print(deck_survival)

# Filter out decks with few passengers for more reliable statistics
reliable_decks = deck_survival[deck_survival['count'] >= 10]
print("\nDecks with 10+ passengers:")
print(reliable_decks)
# %%
# Task 4: Create age groups and combine with class
# Define age groups
def categorize_age(age):
    if pd.isna(age):
        return 'Unknown'
    elif age < 18:
        return 'Child'
    elif age < 35:
        return 'Young Adult'
    elif age < 60:
        return 'Adult'
    else:
        return 'Senior'

df['Age_Group'] = df['age'].apply(categorize_age)

# Combine with passenger class
df['Class_Age_Group'] = (df['pclass'].astype(str) + '_' + 
                          df['Age_Group'])

# Analyze survival by this new feature
survival_by_combo = df.groupby('Class_Age_Group')['survived'].agg([
    'count', 'mean'
]).sort_values('mean', ascending=False)

print("\nSurvival rates by class and age group:")
print(survival_by_combo)
# %%
