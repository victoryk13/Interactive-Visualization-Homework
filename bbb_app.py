import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify, render_template


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///belly_button_biodiversity.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to the measurement and station tables
Otu = Base.classes.otu
Samples = Base.classes.samples
Samples_metadata = Base.classes.samples_metadata

# Create our session (link) from Python to the DB
session = Session(engine)
conn = engine.connect()

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def index():

    return render_template('index.html')


@app.route('/names')
def names():

    results = session.query(Samples).first().__dict__

    names3 = []
    names4 = []

    for key, value in results.items():
        if key != '_sa_instance_state' and key != 'otu_id':
            if len(key) == 6:
                names3.append(key)
            if len(key) == 7:
                names4.append(key)

    names3.sort()
    names4.sort()
    
    names = names3 + names4

    return jsonify(names)


@app.route("/otu")
def otu():

    otu = []

    for result in session.query(Otu.lowest_taxonomic_unit_found).all():
        description = list(np.ravel(result))
        otu.append(description[0])

    return jsonify(otu)


@app.route("/metadata/<sample>")
def metadata(sample):

    sample_number = sample[3:]
    sample_number = int(sample_number)

    for result in session.query(Samples_metadata).all():
        if result.__dict__['SAMPLEID'] == sample_number:
            metadata = result.__dict__
        
    metadata = {k:v for k, v in metadata.items() if k in ('AGE', 'BBTYPE', 'ETHNICITY', 'GENDER', 'LOCATION', 'SAMPLEID')}        

    return jsonify(metadata)


@app.route("/wfreq/<sample>")
def wfreq(sample):

    sample_number = sample[3:]
    sample_number = int(sample_number)

    for result in session.query(Samples_metadata).all():
        if result.__dict__['SAMPLEID'] == sample_number:
            metadata = result.__dict__
        
    wfreq = metadata['WFREQ']

    return jsonify(wfreq)


@app.route("/samples/<sample>")
def samples(sample):

    results = pd.read_sql("SELECT * FROM Samples", conn)

    results = results[['otu_id', sample]]
    results = results.query(sample + " != 0")
    results = results.sort_values(by=sample, ascending=False)

    otu_ids = results['otu_id'].tolist()
    sample_values = results[sample].tolist()

    otu_sample_list = []
    otu_sample_dict = {'otu_ids': otu_ids, 
                    'sample_values': sample_values}
    otu_sample_list.append(otu_sample_dict)

    return jsonify(otu_sample_list)


if __name__ == '__main__':
    app.run()