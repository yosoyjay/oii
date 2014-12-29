function scatter_setup(elt) {
    // first, add features to static HTML
    var plot_options = {
	series: {
	    points: {
		show: true,
		radius: 2,
	    }
	},
	grid: {
	    clickable: true,
	    hoverable: true,
	    autoHighlight: true
	},
	selection: {
	    mode: "xy",
	    color: "red"
	}
    };
    $(elt).bind('show_bin',function(event, bin_pid, view) {
	var endpoint = bin_pid + "_" + view + ".json";
	console.log("Loading "+endpoint+"...");
	$.getJSON(endpoint, function(r) {
	    console.log("Got JSON data");
	    var bin_pid = r.bin_pid;
	    var roi_pids = [];
	    var point_data = [];
	    $.each(r.points, function(ix, point) {
		point_data.push([point.x, point.y]);
		roi_pids.push(bin_pid + '_' + point.roi_num);
	    });
	    var plot_data = {
		label: view,
		data: point_data,
		color: "black",
		points: {
		    fillColor: "black"
		},
		highlightColor: "red"
	    };
	    $(elt).empty().plot([plot_data], plot_options);
	    $(elt).data('roi_pids', roi_pids);
	    $(elt).data('point_data', point_data);
	});
	$(elt).bind("plotselected", function(evt, ranges) {
	    console.log(evt);
	    $(evt.target).addClass("plotselected");
	    // determine which rois are selected and fire an event?
	    lo_x = ranges.xaxis.from;
	    hi_x = ranges.xaxis.to;
	    lo_y = ranges.yaxis.from;
	    hi_y = ranges.yaxis.to;
	    $.each($(elt).data('point_data'), function(ix, point) {
		var x = point[0];
		var y = point[1];
		if(x >= lo_x && x <= hi_x && y >= lo_y && y <= hi_y) {
		    var pid = $(elt).data('roi_pids')[ix];
		    console.log('selected '+pid);
		}
	    });
	});
	$(elt).bind("plotclick", function(evt, pos, item) {
	    console.log(evt);
	    if($(evt.target).hasClass("plotselected")) {
		$(evt.target).removeClass("plotselected");
		return;
	    } else if(item) {
		var roi_pids = $(elt).data('roi_pids');
		if(!roi_pids) { return };
		roi_pid = roi_pids[item.dataIndex];
		// fire a roi clicked event
		$(elt).trigger('roi_click',roi_pid);
	    }
	})
    });
}
//jquery plugin
(function($) {
    $.fn.extend({
	scatter: function() {
	    return this.each(function() {
		scatter_setup($(this));
	    });//each
	}//scatter
    });//$.fn.extend
})(jQuery);//end of plugin
