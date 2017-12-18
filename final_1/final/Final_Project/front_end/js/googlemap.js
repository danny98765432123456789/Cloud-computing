var map;

function initMap() {

  map = new google.maps.Map(document.getElementById('map'), {
    zoom: 14,
    center: new google.maps.LatLng(25.019303, 121.542425),
    mapTypeId: 'terrain'
  });
}

$.ajax("http://140.112.41.157:3001/googlemap/googlemap/", {
  type: 'GET',
  success: function(results) {
    alert("總資料數： "+results.length + " 筆"+ " \n" +"資料來源： 新北市政府");

    for (var i = 0; i < results.length; i++) {
      var latLng = new google.maps.LatLng(results[i].park_longitude, results[i].park_latitude);
      var marker = new google.maps.Marker({
        position: latLng,
        map: map
      });
      var infowindow = new google.maps.InfoWindow();
      let contentString =
        '<div id="bodyContent >' +
        '<div id="content">' +
        '<div id="siteNotice" style="font-size:30px" >' + results[i].park_name +
        '</div>' +
        '<div style="font-size:20px" >' + '<p ><b>剩餘車位: </b>' + results[i].remain_car +
        '</p>' + '</div>' +
        '<div style="font-size:20px" >' + '<p ><b>計費方式: </b>' + results[i].park_payex +
        '</p>' + '</div>' +
        '<div style="font-size:20px" >' + '<p ><b>營業時間: </b>' + results[i].park_servicetime +
        '</p>' + '</div>' +
        '<div style="font-size:20px" >' + '<p ><b>地址資訊: </b>' + results[i].park_address +
        '</p>' + '</div>' +

        '</div>'
      google.maps.event.addListener(marker, 'click', (function(marker, i) {
        return function() {
          infowindow.setContent(contentString);
          infowindow.open(map, marker);
        }
      })(marker, i));
    }
  }
})
