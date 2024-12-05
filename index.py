from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
import copy
from db_config import mongo_uri, database_name, collection_name

app = Flask(__name__)

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

cached_result = {}
prev_filters = {
  "start_date" : None,
  "end_date" : None,
  "facilities" : [],
}

@app.route('/emissions', methods=['GET'])
def get_emissions():
    global cached_result
    try:
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        facilities = request.args.getlist('BUSINESS_FACILITY')
        if not start_date or not end_date or not facilities:
            return jsonify({"error": "Missing required parameters"}), 400
        
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        if(prev_filters['start_date'] == start_date and prev_filters['end_date'] == end_date and prev_filters['facilities'] == facilities):
          return jsonify(cached_result), 200
        
        overlap_flag = False
        if(prev_filters['start_date'] is not None and start_date > prev_filters['start_date'] and start_date < prev_filters['end_date'] and end_date > prev_filters['end_date']):
            start_date = prev_filters['end_date'] + timedelta(days=1)
            overlap_flag = True
        elif(prev_filters['start_date'] is not None and end_date > prev_filters['start_date'] and end_date < prev_filters['end_date'] and start_date < prev_filters['start_date']):
            end_date = prev_filters['start_date'] - timedelta(days=1)
            overlap_flag = True

        total_facilities = facilities
        if(len(prev_filters['facilities']) > 0 and prev_filters['facilities'] != facilities and len(facilities) > len(prev_filters['facilities'])):
            facilities = [facility for facility in facilities if facility not in prev_filters['facilities']]            
            overlap_flag = True

        qry = {
            "TRANSACTION DATE": {"$gte": start_date, "$lte": end_date},
            "Business Facility": {"$in": list(facilities)}
        }
        pipeline = [
            {"$match": qry},
            {"$group": {
                "_id": "$Business Facility",
                "total_emissions": {"$sum": "$CO2_ITEM"}
            }}
        ]
        db_result = list(collection.aggregate(pipeline))

        merged_result = [];
        if(overlap_flag):
            for facility in total_facilities:
                total_emissions = 0;
                if(facility in prev_filters["facilities"]):
                    match = next((item for item in cached_result["data"] if item["_id"] == facility), None)
                    prev_emissions = match["total_emissions"] if match else 0
                    total_emissions = total_emissions + prev_emissions
                if(facility in facilities):
                    match = next((item for item in db_result if item["_id"] == facility), None)
                    prev_emissions = match["total_emissions"] if match else 0
                    total_emissions = total_emissions + prev_emissions
                merged_result.append({"_id" : facility, "total_emissions": total_emissions})

        response = {
            "startDate": start_date,
            "endDate": end_date,
            "data": db_result if not overlap_flag else merged_result
        }
        cached_result = copy.copy(response)
        prev_filters["start_date"] = start_date
        prev_filters["end_date"] = end_date
        prev_filters["facilities"] = total_facilities
        
        return jsonify(response), 200

    except Exception as e:
        print(str(e))
        return jsonify({"error": "Internal Server Error"}), 500


if __name__ == '__main__':
    app.run(debug=True)
