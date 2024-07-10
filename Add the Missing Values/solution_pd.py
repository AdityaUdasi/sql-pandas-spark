import pandas as pd
import numpy as np

# Create a sample DataFrame
data = {'row_id': [x for x in range(1,13)],
        'job_role': ['Data Engineer',np.nan,np.nan, np.nan,np.nan,'Web Developer',np.nan,np.nan, 'Data Scientist',np.nan,np.nan,np.nan],
        'skills': ['SQL','Python','AWS','Snowflake','Apache Spark','Java','HTML','CSS','Python','Machine Learning','Deep Learning','Tableau']
    }

df = pd.DataFrame(data)
df['job_role'] = df['job_role'].fillna(method='ffill')
print(df)