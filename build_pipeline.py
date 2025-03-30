from azure.ai.ml import command
from azure.ai.ml.dsl import pipeline
from azure.ai.ml.entities import Environment, BuildContext

from core import DataSchema
from core.build_ import Env, build_infofile
from core.ds_constants import get_ml_client, get_secret

# 0. Pipe info
COMPUTE = "goodboy"
ENV_NAME = "sdk2-env-basic"
PIPELINE_NAME = "AML-SDK2-Template"
DESCRIPTION = "basic pipeline example on azure ml sdk v2"
EXPERIMENT_NAME = "DebugTestSDK2DockerUV"

# 1. Auth, feel free yourself to using your auth method
client = get_ml_client()


# 2. DEFINE ENV
build_context = BuildContext(
    path=".",
    dockerfile_path="./amlenv.Dockerfile")

env = Environment(
    name='test_'+ ENV_NAME + "_docker",
    build=build_context,
    description="test docker based env",
)
env = client.environments.create_or_update(env)


# 3. Define DataSchema's
laptops = DataSchema(default_value="teststorage:test_datasets/toy datasets/laptop_price.csv", data_type="uri_file")


model = DataSchema(
    default_value="teststorage:test_datasets/models/laptop_price_model.pkl",
    data_type="uri_file",
    description="trained pickle model",
)

# 4. DEFINE COMPONENTS:
prep_component = command(
    name="preprocess",
    description="preprocess passed 'laptop_price_data' data",
    inputs={"laptop_price_data": laptops.as_input()},
    outputs={
        "preprocessed_laptops_data": laptops.as_output(value="teststorage:test_datasets/preprocessed_laptops.csv"),
    },
    environment=env,
    code="src/",
    command="""python components/preprocess/main.py \
            --laptop_price_data ${{inputs.laptop_price_data}}\
            --preprocessed_laptops_data ${{outputs.preprocessed_laptops_data}}
            """,
    is_deterministic=False,
)


train_component = command(
    name="train",
    description="train model on passed 'preprocessed_laptops_data' data.",
    inputs={
        "preprocessed_laptops_data": laptops.as_input("teststorage:test_datasets/preprocessed_laptops.csv"),
        "test_size": 0.15,
    },
    outputs={
        "trained_model": model.as_output(),
    },
    environment=env,
    code="src/",
    command="""python components/train/main.py \
            --preprocessed_laptops_data ${{inputs.preprocessed_laptops_data}}\
            --test_size ${{inputs.test_size}}\
            --trained_model ${{outputs.trained_model}}
            """,
    is_deterministic=False,
)


predict_component = command(
    name="predict",
    description="predict 'laptops_to_predict' data using 'trained_model' model.",
    inputs={
        "trained_model": model.as_input(),
        "laptops_to_predict": laptops.as_input(value="teststorage:test_datasets/toy datasets/laptop_price_clone.csv"),
    },
    outputs={
        "prediction_data": laptops.as_output(value="teststorage:test_datasets/model_prediction.csv"),
    },
    environment=env,
    environment_variables={"MAIL_PASSWORD": get_secret("loy-fraud-secret-email-app-password")},
    code="src/",
    command="""python components/predict/main.py \
            --laptops_to_predict ${{inputs.laptops_to_predict}}\
            --trained_model ${{inputs.trained_model}}\
            --prediction_data ${{outputs.prediction_data}}
            """,
    is_deterministic=False,
)


# 5. DEFINE PIPELINES
@pipeline(name=PIPELINE_NAME, display_name=PIPELINE_NAME, description=DESCRIPTION, default_compute=COMPUTE)
def train_model(laptop_price_data):
    prep = prep_component(laptop_price_data=laptop_price_data)
    train = train_component(preprocessed_laptops_data=prep.outputs.preprocessed_laptops_data)
    trained_model = train.outputs.trained_model
    return {"trained_model": trained_model}


@pipeline(
    name="predict_laptops_example",
    display_name="predict_laptops_example",
    description="predict_laptops_example",
    default_compute=COMPUTE,
)
def predict(laptops_to_predict, model):
    preprocessed_data = prep_component(laptop_price_data=laptops_to_predict).outputs.preprocessed_laptops_data
    prediction = predict_component(laptops_to_predict=preprocessed_data, trained_model=model).outputs.prediction_data
    return {"prediction": prediction}


@pipeline(
    name="train_and_predict_laptops_example",
    display_name="Train model and predict for laptops",
    description="some descr-ve descr...",
    default_compute=COMPUTE,
)
def train_predict(laptop_price_data, laptop_price_data_test):
    prep = prep_component(laptop_price_data=laptop_price_data)
    train = train_component(preprocessed_laptops_data=prep.outputs.preprocessed_laptops_data)
    trained_model = train.outputs.trained_model

    preprocessed_data = prep_component(laptop_price_data=laptop_price_data_test).outputs.preprocessed_laptops_data
    prediction = predict_component(
        laptops_to_predict=preprocessed_data, trained_model=trained_model
    ).outputs.prediction_data
    return {"prediction": prediction}


if __name__ == "__main__":
    # 6. call pipeline - build jobs
    train_predict_job = train_predict(
        laptop_price_data=laptops.as_input(),
        laptop_price_data_test=laptops.as_input("teststorage:test_datasets/toy datasets/laptop_price_clone.csv"),
    )

    ##------------------------------------------------------------------------------------------------------------|

    # submit train-predict job
    debug = input("Start as Debug experiment? (y/n): ").lower().strip() in ("y", "yes")
    # set to debug
    EXPERIMENT_NAME = f"Debug{EXPERIMENT_NAME}" if debug else EXPERIMENT_NAME

    # generate the info-file
    build_infofile(
        pipeline_name=PIPELINE_NAME, experiment_name=EXPERIMENT_NAME, pipeline_description=DESCRIPTION, save_at="src/"
    )

    # create the job
    create_model_job = client.jobs.create_or_update(
        job=train_predict_job, experiment_name=EXPERIMENT_NAME, description=DESCRIPTION
    )
    client.jobs.stream(create_model_job.name)
