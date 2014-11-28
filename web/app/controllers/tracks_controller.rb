class TracksController < ApplicationController
  before_action :authenticate_user!

  def index
    @dbdata = "Uploadad files"
    @entries = Track.where(user: current_user)
    @db_id = "dataset-user"
    @dbname = current_user.db_name

    respond_to do |format|
      format.html
      format.js { render "datasets/show" }
    end
  end

  def new
    @track = Track.new
  end

  def create
    unless params.has_key?(:track) and params[:track].has_key?(:fname)
      redirect_to(:back, {:alert => 'Please provide file.'})
    else
      file = params[:track][:fname]
      @track = Track.new(:fname => file.original_filename, :fsize => file.size, :user => current_user)
      @track.add_user_file(file, current_user)

      if @track.save
        redirect_to({action: 'index'}, {:notice => "#{@track.fname} is saved."})
      else
        redirect_to(:back, {:alert => @track.errors.to_a[0]})
      end
    end
  end

  def destroy
    @track = current_user.track.find(params[:id])
    @track.destroy

    if @track.destroyed?
      redirect_to({action: 'index'}, {:notice => "#{@track.fname} is deleted."})
    else
      redirect_to({action: 'index'}, {:alert => @track.errors.to_a[0]})
    end
  end

  def update
    @track = current_user.track.find(params[:id])
    if @track.update(permit_params)
      flash[:notice] = "#{@track.fname} is renamed."
    else
      flash[:alert] = @track.errors.to_a[0]
    end

    respond_to do |format|
      format.js
    end
  end

  private
  def send_error(msg)
    flash[:error] = msg
    redirect_to action: 'new'
    return
  end

  def permit_params
    params.require(:track).permit(:fname)
  end
end
