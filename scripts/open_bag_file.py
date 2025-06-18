import bagPy
import pathlib
import datetime
import shutil

import bagMetadataSamples as MetdataSamples


INPUTS = pathlib.Path(__file__).parents[1] / 'inputs'
OUTPUTS = pathlib.Path(__file__).parents[1] / 'outputs'


def open_bag_file(bag_file_path):
    dataset = bagPy.Dataset.openDataset(bag_file_path, bagPy.BAG_OPEN_READ_WRITE)
    # print(dir(dataset))
    return dataset


def get_definition():
    definition = bagPy.METADATA_DEFINITION_NOAA_OCS_2022_10
    return definition


def get_metadata():
    metadata = bagPy.Metadata()
    metadata.loadFromBuffer(MetdataSamples.kMetadataXML)

    return metadata


def get_layers(dataset):
    layers = []
    print('Layer types:')
    print(dataset.getLayerTypes())

    print('Found layers:')
    for layer in dataset.getLayers():
        # print(dir(layer))
        print('\t', layer.getDescriptor().getName())
        layers.append(layer)
    return layers


def create_layer(dataset):
    definition = get_definition()
    # 'indexType', 'profile', 'name', 'definition', 'chunkSize', and 'compressionLevel'
    print(bagPy.Georef_Metadata)
    print(dir(bagPy.Georef_Metadata))
    georefMetadataLayer = dataset.createGeorefMetadataLayer(bagPy.DT_UINT8, bagPy.NOAA_OCS_2022_10_METADATA_PROFILE, bagPy.Georef_Metadata, definition, 100, 6)
    print('Created Metadata Layer')
    get_layers(dataset)


def update_metadata(bag_file_path):
    output_bag = OUTPUTS / 'edited_bag.bag'
    if output_bag.is_file():
        output_bag.unlink()
    shutil.copy(str(bag_file_path), str(output_bag))

    dataset = open_bag_file(str(output_bag))
    layers = get_layers(dataset)
    for layer in layers:
        info = layer.getDescriptor()
        if 'Metadata' in info.getName():
            # print(dir(layer.getDescriptor()))
            # 'getChunkSize', 'getCompressionLevel', 'getDataType', 'getElementSize', 'getId', 'getInternalPath', 
            # 'getLayerType', 'getMinMax', 'getName', 'setMinMax', 'setName', 'this', 'thisown'
            print('\n', info.getName())
            # print(dir(layer))
            # 'getDataType', 'getDescriptor', 'getElementSize', 'getInternalPath', 'read', 'this', 'thisown', 'write', 'writeAttributes'
            # info.setName('NOAA_Metdata')  # Variable_Resolution_Metadata

            # TODO try to add new attributes?


def process():

    # bag_georefmetadata_layer.bag
    #      Elevation
    #      Uncertainty
    #      Elevation
    # example_w_qc_layers.bag
    #     bagPy.ErrorLoadingMetadata
    # metadata_layer_example.bag
    #     OSError: [Errno 0] Error
    # nominal_only.bag
    #     bagPy.ErrorLoadingMetadata
    # sample-1.5.0.bag
    #     bagPy.ErrorLoadingMetadata
    # sample-2.0.1.bag
    #      Elevation
    #      Uncertainty
    #      Nominal_Elevation
    #      Surface_Correction
    # true_n_nominal.bag
    #     bagPy.ErrorLoadingMetadata

    # bag_file_path = INPUTS /  'H13667_MB_VR_Ellipsoid_1of1.bag'
    # bag_file_path = INPUTS /  'sample' / 'sample-2.0.1.bag'  # 2.0.1 opens fine
    bag_file_path = INPUTS /  'sample' / 'sample-1.5.0.bag'  # 1.5.0 fails for extra content at the end of the document
    # update_metadata(bag_file_path)

    # bag_file_path = INPUTS / 'sample' / 'true_n_nominal.bag'
    dataset = open_bag_file(str(bag_file_path))
    # layers = get_layers(dataset)


if __name__ == "__main__":
    process()
