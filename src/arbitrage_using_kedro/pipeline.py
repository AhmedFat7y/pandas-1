# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Pipeline construction."""

from kedro.pipeline import Pipeline, node

import arbitrage_using_kedro.nodes.arbitrage as nodes

# Here you can define your data-driven pipeline by importing your functions
# and adding them to the pipeline as follows:
#
# from nodes.data_wrangling import clean_data, compute_features
#
# pipeline = Pipeline([
#     node (clean_data, 'customers', 'prepared_customers'),
#     node (compute_features, 'prepared_customers', ['X_train', 'Y_train'])
# ])
#
# Once you have your pipeline defined, you can run it from the root of your
# project by calling:
#
# $ kedro run
#


def create_pipeline(**kwargs):
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        Pipeline: The resulting pipeline.

    """

    ###########################################################################
    # Here you can find an example pipeline with 4 nodes.
    #
    # PLEASE DELETE THIS PIPELINE ONCE YOU START WORKING ON YOUR OWN PROJECT AS
    # WELL AS THE FILE nodes/example.py
    # -------------------------------------------------------------------------

    pipeline = Pipeline(
        [
            node(
                nodes.save_data_files_paths_to_csv,
                ["params:raw_folder"],
                "files_df",
            ),
            node(
                nodes.combine_raw_multiple_json_to_single_df,
                ["files_df", "params:needed_obj_keys"],
                "cleaned_df",
            ),
            node(
                nodes.generate_from_to_columns,
                ["cleaned_df", "params:supported_currencies_list"],
                "base_edges_df",
            ),
            node(
                nodes.cast_columns,
                "base_edges_df",
                "casted_base_edges_df",
            ),
            node(
                nodes.generate_edges,
                ["casted_base_edges_df"],
                "all_edges_df",
            ),
            node(
                nodes.generate_cycles_df,
                ["all_edges_df"],
                "cycles_df"
            ),
            node(
                nodes.generate_arbitrage_df,
                ["cycles_df"],
                "arbitrage_df",
            ),
        ]
    )
    ###########################################################################

    return pipeline
