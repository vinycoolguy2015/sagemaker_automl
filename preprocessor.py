import json
import traceback

def preprocess_handler(inference_record, logger):
    """
    Robust preprocessor: handles dicts and object wrappers (CapturedData).
    Ensures correct types for all fields and outputs JSON for monitoring.
    """
    record = inference_record
    logger.debug(f"I'm debugging a processing record: {record}")

    # Default flattened record
    flat_record = {
        "price": 0.0,
        "sqft": 0.0,
        "bedrooms": 0,
        "bathrooms": 0.0,
        "location": "",
        "year_built": 0,
        "condition": ""
    }

    # --- Helpers ---
    def get_prop(obj, name):
        """Return obj[name] / obj.get(name) / getattr(obj, name) / None safely."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(name)
        if hasattr(obj, name):
            try:
                return getattr(obj, name)
            except Exception:
                pass
        try:
            return obj[name]
        except Exception:
            return None

    def extract_data(container):
        """Return underlying data (string/dict/bytes) from container (dict or object)."""
        if container is None:
            return None
        val = get_prop(container, "data") or container
        if isinstance(val, (bytes, bytearray)):
            try:
                return val.decode("utf-8")
            except Exception:
                return str(val)
        return val

    try:
        # --- Capture input ---
        capture = get_prop(record, "captureData") or get_prop(record, "capturedata") or record
        logger.debug(f"capture object type: {type(capture)}")

        # Input CSV
        endpoint_input = get_prop(capture, "endpointInput") or get_prop(capture, "endpoint_input")
        csv_raw = extract_data(endpoint_input)
        logger.debug(f"endpoint_input type: {type(endpoint_input)}, data type: {type(csv_raw)}, value: {csv_raw}")

        if isinstance(csv_raw, str):
            fields = [f.strip() for f in csv_raw.split(",") if f.strip() != ""]
            logger.debug(f"CSV fields parsed: {fields} (count={len(fields)})")
            if len(fields) >= 6:
                try:
                    flat_record.update({
                        "sqft": float(fields[0]),
                        "bedrooms": int(fields[1]),
                        "bathrooms": float(fields[2]),
                        "location": str(fields[3]),
                        "year_built": int(fields[4]),
                        "condition": str(fields[5]),
                    })
                except Exception as e:
                    logger.error(f"CSV field conversion error: {e}\n{traceback.format_exc()}")
        else:
            logger.debug("CSV raw is not a str — skipping CSV parse.")

    except Exception as e:
        logger.error(f"Preprocessor input parse error: {e}\n{traceback.format_exc()}")

    try:
        # --- Prediction output ---
        endpoint_output = get_prop(capture, "endpointOutput") or get_prop(capture, "endpoint_output")
        pred_raw = extract_data(endpoint_output)
        logger.debug(f"endpoint_output type: {type(endpoint_output)}, data type: {type(pred_raw)}, value: {pred_raw}")

        pred_json = None
        if isinstance(pred_raw, str):
            try:
                pred_json = json.loads(pred_raw)
            except Exception:
                pred_json = None
        elif isinstance(pred_raw, (dict, list)):
            pred_json = pred_raw
        elif pred_raw is not None:
            try:
                pred_json = json.loads(str(pred_raw))
            except Exception:
                pred_json = None

        logger.debug(f"Prediction parsed: {pred_json}")

        if isinstance(pred_json, dict):
            predictions = pred_json.get("predictions", [{}])
            if isinstance(predictions, list) and len(predictions) > 0:
                score = predictions[0].get("score")
                if score is not None:
                    flat_record["price"] = float(score)

    except Exception as e:
        logger.error(f"Preprocessor output parse error: {e}\n{traceback.format_exc()}")

    # --- Ensure all types are correct before returning ---
    try:
        flat_record["sqft"] = float(flat_record.get("sqft", 0.0))
        flat_record["bathrooms"] = float(flat_record.get("bathrooms", 0.0))
        flat_record["bedrooms"] = int(flat_record.get("bedrooms", 0))
        flat_record["year_built"] = int(flat_record.get("year_built", 0))
        flat_record["price"] = float(flat_record.get("price", 0.0))
        flat_record["location"] = str(flat_record.get("location", ""))
        flat_record["condition"] = str(flat_record.get("condition", ""))
    except Exception as e:
        logger.error(f"Type casting error: {e}\n{traceback.format_exc()}")

    logger.debug(f"I'm debugging a processing record 2: {list(flat_record.keys())}")
    logger.debug(f"I'm debugging a processing record 3: {flat_record}")

    values = [str(flat_record['price']), str(flat_record['sqft']), str(flat_record['bedrooms']), 
              str(flat_record['bathrooms']), flat_record['location'], str(flat_record['year_built']), 
              flat_record['condition']]
    return {str(i).zfill(20): v for i, v in enumerate(values)}
