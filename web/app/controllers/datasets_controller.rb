class DatasetsController < ApplicationController
  def index
    @datasets = Dataset.all
  end

  def show
    @datasets = Dataset.all
    @dataset = Dataset.find(params[:id])

    @dbname = @dataset.dbname
    @dbdata = @dataset.data
    @entries = Dataset.get_entries(params[:id])
    @db_id = "dataset-#{params[:id]}"

    respond_to do |format|
      format.html
      format.js
    end    
  end
end
