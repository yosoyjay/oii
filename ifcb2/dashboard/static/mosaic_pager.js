// jQuery plugin extending image_pager for IFCB mosaic images.
// obv requires image_pager.
// fires the following events
// page_change(pageNumber, page_url) - when the user changes the page; generally not used
// roi_click(roi_pid, width, height) - when the user clicks a ROI
// goto_bin(pid) - when the user clicks on the "next/prev" bin button
(function($) {
    $.fn.extend({
	mosaicPager: function(timeseries, pid, width, height) {
	    var BIN_URL='mosaic_bin_url';
	    // pid - bin pid (or lid)
	    // width -  the width  \__of the mosaic
	    // height - the height / 
	    var roi_scale = 0.33;
	    return this.each(function () {
		var $this = $(this); // retain ref to $(this)
		$this.css('width',width+100)
		    .css('height',height);
		// put 30 pages on the pager, just in case it's a huge bin
		// FIXME somehow figure out how many pages there are re #1701
		var images = [];
		for(var page=1; page <= 30; page++) {
		    // each page is a mosaic API call URL with a successive page number
		    var url = '/'+timeseries+'/api/mosaic/size/'+width+'x'+height+'/scale/'+roi_scale+'/page/'+page+'/pid/'+pid+'.jpg';
		    images.push(url);
		}
		$.getJSON(pid+'_medium.json', function(r) {
		    $this.find('.imagepager_rois_total').empty().append(r.targets.length+'');
		    $this.find('.imagepager_date').attr('title',r.date).timeago();
		});
		// list of images in hand, create the image pager
		$this.empty().append('<div class="mosaic_pager_image_pager"></div>')
		    .append('<div class="imagepager_paging">page <span class="imagepager_page_number"></span>, showing <span class="imagepager_rois_shown">?</span> of <span class="imagepager_rois_total">?</span> target(s)</div>')
		    .find('div.mosaic_pager_image_pager').imagePager(images, width, height) // use the image pager plugin
		    .bind('change', function(event, ix, image_href) { // when the user changes which page they're viewing
			$this.data(BIN_URL, image_href);
			$this.find('.imagepager_page_number').empty().append((ix+1)+'');
			$this.find('.imagepager_rois_shown').empty().append('&#x21BB;');
			$this.trigger('page_change', [ix+1, image_href]);
			$.getJSON(image_href.replace('.jpg','.json'), function(r) {
			    $this.find('.imagepager_rois_shown').empty().append(r.length+'');
			});
		    }).delegate('.page_image', 'click', function(event) { // when the user clicks on the mosaic image
			// figure out where the click was
			var clickX = event.pageX - $(this).offset().left;
			var clickY = event.pageY - $(this).offset().top;
			// now figure out which ROI the click was in. that requires the layout, see below
			function roi_click(layout) {
			    $.each(layout, function(ix, tile) {
				if(clickX >= tile.x && clickX <= tile.x + tile.width &&
				   clickY >= tile.y && clickY <= tile.y + tile.height) {
				    console.log('user clicked on '+tile.pid);
				    setTimeout(function() {
					$this.trigger('roi_click', tile.pid);
				    }, 0);
				}
			    });
			}
			// fetch it from the mosaic page image URL except with the extension "json"
			var mosaic_image_href = $this.data(BIN_URL);
			$.getJSON(mosaic_image_href.replace('jpg','json'), function(layout) {
			    roi_click(layout); // now figure out where the user clicked
			});
		    });
	    });//each in mosaicPager
	},//mosaicPager
    });//$.fn.extend
})(jQuery);//end of plugin
