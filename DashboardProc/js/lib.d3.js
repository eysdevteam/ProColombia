/***************************** 
	Libreria D3.js
*////////////////////////////

// Table Library
function table(dataset, columnas, container){  

  var tbody = d3.select(container).append('tbody');

  var rows = tbody.selectAll("tr")
    .data(dataset)
    .enter()
    .append("tr")
    .text(function(column) { //return column;
       return column.id + " " + column.name;
     });

  var cells = rows.selectAll("td")
    .data(function(row){
      return columnas.map(function(column){
        return {column:column, value:row[column]};
      });
    })
    .enter()
    .append("td")

    return tbody;
}

// Donut Library
function donut(dataset, container) {
  var width = 100;
      height = 150,
      radius = Math.min(width, height) / 2;
     
if (container=="#IPSmejoraron") {
  var color  = d3.scale.ordinal()
      .range(["rgb(60, 85, 164)","#EBE8E8"]);
    }
    else
    {
      var color  = d3.scale.ordinal()
      .range(["rgb(165, 0, 38)","#EBE8E8"]);
    }

  var pie = d3.layout.pie()
      .sort(null);

  var arc = d3.svg.arc()
      .innerRadius(radius - 5)
      .outerRadius(radius - 10);

  var svg = d3.select(container).append("svg")
      .attr("width", width)
      .attr("height", height)
      .append("g")
      .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

  var path = svg.selectAll("path")
      .data(pie(dataset))
      .enter().append("path")
      .attr("fill", function(d, i) { return color(i); })
      .attr("d", arc);

  svg.append("text")
    .text(dataset[0]+"/"+dataset[1])
    .attr("class", "units-label")
    .attr("x", radius/20-25)
    .attr("y", radius/5)
    .attr("font-size", 25);
}

// Tabla Library 
function tabla(container){
    d3.json("web/tablebest/table.json", function(error, data) {
        var datasetmal = data; 
        d3.select(".titulos").text("Top - Mejores IPS");
        d3.select(container).selectAll("tr").data(datasetmal).enter().append("tr").attr("class","ipsre");
        d3.selectAll(".ipsre").append("td").attr("class",function(d,i){return "id";});
        d3.selectAll(".ipsre").append("td").attr("class",function(d,i){return "ips";});
        d3.selectAll(".ipsre").append("td").attr("class",function(d,i){return "goodbad";});
        d3.selectAll(".id").data(datasetmal).text(function(d){return d.id;});
        d3.selectAll(".ips").data(datasetmal).text(function(d){return d.name.toLowerCase();});
        d3.selectAll(".goodbad").append("i").attr("class","fa fa-chevron-circle-up greencolor");

        d3.select(".goodbutton").on("click",function(){
            d3.json("web/tableworst/table.json", function(error, data) {
                var dataset = data; 
                d3.select(".titulos").text("Top - IPS Deficientes");
                d3.selectAll(".id").data(dataset).text(function(d){return d.id;});
                d3.selectAll(".ips").data(dataset).text(function(d){return d.name.toLowerCase();});
                d3.selectAll("i").attr("class","fa fa-chevron-circle-down redcolor");
            });
        });
    });

    d3.select(".badbutton").on("click",function(){
        d3.json("web/tablebest/table.json", function(error, data) {
            var datasetmal = data; 
            d3.select(".titulos").text("Top - Mejores IPS");
            d3.selectAll(".id").data(datasetmal).text(function(d){return d.id;});
            d3.selectAll(".ips").data(datasetmal).text(function(d){return d.name.toLowerCase();});
            d3.selectAll("i").attr("class","fa fa-chevron-circle-up greencolor");
        });
    });
}


// Tablavar Library
function tablavar(data,container){
    //d3.json("web/name-var/name.json", function(error, data) {
    var dataset = data; 
    d3.select(".card-title").text("Variables analizadas");
    d3.select(container).selectAll("tr").data(dataset).enter().append("tr").attr("class","var1");
    d3.selectAll(".var1").append("td").attr("class",function(d,i){return "numvar";});
    d3.selectAll(".var1").append("td").attr("class",function(d,i){return "var2";});
    d3.selectAll(".numvar").data(dataset).text(function(d){return d.var;});
    d3.selectAll(".var2").data(dataset).text(function(d){return d.name.toLowerCase();});
    
//});
}

// Scatter Treemap and HeatMap library
function scatreeheat(data, container) {
  var margin = {
    top: 20, 
    right:30, 
    bottom: 20, 
    left: 30,
    padding: 20,
    padding2: 40,
    padding3: 28
  },
  height = 275 - margin.top - margin.bottom;
  width = parseInt(d3.select(container).style("width")) - margin.left - margin.right;

  var color = d3.scaleSequential(d3.interpolateRdYlBu).domain([100, 0])

  var x = d3.scale.linear()
    .domain([d3.min(data, function(d){return d.cx-15; }), d3.max(data, function(d) { return d.cx+15; })])
    .range([ 0, width ]);

  var y = d3.scale.linear()
    .domain([d3.max(data, function(d){return d.cm+5}), 0])
    .range([0, height]);

  var yAxis = d3.axisLeft().scale(y);

  var legend = d3.select(container)
    .append('svg:svg')
    .attr('width', width + margin.right + margin.left)
    .attr('height', 15)
    .style("padding-left", "4%")

  var image = legend.append("svg:image")
    //attr("transform", 'translate(' + margin.left + ',' + margin.top + ')')
    .attr('width', width)
    .attr('height', 15)
    .attr("xlink:href","legendV2.png")    

  var chart = d3.select(container)
    .append('svg:svg')
    .attr('width', width + margin.right + margin.left)
    .attr('height', height + margin.top + margin.bottom);

  var axisLeft = chart.append("g")
    .call(yAxis)
    .attr("transform", 'translate(' + margin.left + ',' + margin.top + ')')

  var textAxisLeft = chart.append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", ((height/2)+margin.top+margin.bottom)*-1)
    .attr("y", 10)
    .style("font-size", "0.8em")
    .text("Cantidad de IPS")
    .attr("fill","gray");

  var main = chart.append('g')
    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
    .attr('width', width + margin.right + margin.left)
    .attr('height', height + margin.top + margin.bottom);

  var g = main.append("svg:g");

  g.selectAll("dots")
    .data(data)
    .enter()
    .append("svg:circle")
    .style("cursor","pointer")
    .attr("cx", function (d,i) { return x(d.cx); } )
    .attr("cy", function (d) { return y(d.cm); } )
    .attr("r", function (d) { return d.cm + 30; } )
    .attr("class", "dots")
    .attr("data-toggle", "modal")
    .attr("data-target", "#myModal")
    .attr("stroke", "white")
    .attr("fill", function(d) { return color(d.cx); })
    .on("mouseover", function(d,i) {
        var radio = d3.select(this)
        .transition()
        .duration(200)
        .attr("r", function(d) { return d.cm + 35});
    })
    .on("mouseout", function(d,i) {
        var radio = d3.select(this)
        .transition()
        .attr("r", function(d) { return d.cm + 30});
    })
    .on("click", function(d) {
        d3.selectAll(".trModal").remove();
        d3.selectAll("tdModal").remove();

        let modal_title = d3.selectAll(".modal-title-scatter")
        modal_title.text("Este grupo tiene " + d.cm + " IPS");
        d3.select(this).each(function(d) {
          d.m.forEach((ips, index) => {
            d3.selectAll(".modal-scatter")
              .append("tr")
              .attr("class", "trModal")
              .append("td")
              .attr("class", "tdModal")
              .text(ips)
              .style("border-bottom","1px solid #EBE8E8")
          })
        })
    })
    .append("title").text("Click para mas informacion")
    d3.selectAll(".trModal").remove();
    d3.selectAll("tdModal").remove();  
       
  g.selectAll("text")
    .data(data)
    .enter()
    .append("svg:text") 
    .attr("x", function (d,i){
      if(d.cm < 10){ return x(d.cx-2.5) }
      else{ return x(d.cx-5) }
    })
    .attr("y", function (d,i) { return y(d.cm-0.15); } )   
    .text(function(d) { return d.cm + " IPS"; } )    
    .attr("fill", "black")
    .style("font-weight","600")
    //.style("text-shadow","1px 1px white");    
}

// Bargraph Library
function bargraph(data,container, container2){

    // set the dimensions and margins
    var margin = {top: 20, right: 20, bottom: 30, left: 40},
        width = parseInt(d3.select(container).style("width")) - margin.left - margin.right;
        height = 190 - margin.top - margin.bottom;

    // set the ranges
    var x = d3.scaleBand()
              .range([0, width])
              .padding(0.4);
    var y = d3.scaleLinear()
              .range([height, 0]);
              

    var svg = d3.select(container2).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", 
              "translate(" + margin.left + "," + margin.top + ")");

    // get the data
    //d3.json("web/bars-whisker/table.json", function(error, data) {
    //  if (error) throw error;

     data.forEach(function(d) {
        d.mean = +d.mean;
      });
      var color = d3.scaleSequential(d3.interpolateRdYlBu).domain([100, 0])
       x.domain(data.map(function(d) { return d.name; }));
       y.domain([0, 100]);

      var titulo="Variables de estudio";

  svg.append("text")
      .text(titulo)
      .attr("x",width/2 - margin.left)
      .attr("y",height+margin.bottom)
      .attr("fill","gray")
      .style("font-size",12);

 
  svg.append("text")
      .attr("transform","rotate(-90)")
      .text("valor de referencia")
      .attr("x",((height/2)+margin.top+margin.bottom)*-1)
      .attr("y", margin.bottom*-1)
      .attr("fill","gray")
      .style("font-size",12);

    var bar =svg.selectAll(".bar")
      .data(data)
      .enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return x(d.name); })
      .attr("width", x.bandwidth())
      .attr("y", function(d) { return y(0); })
      .attr("height", function(d) { return 0 })
      .attr("fill", function(d){ return color(d.mean);})

      .transition()
      .ease(d3.easeBounce)
      .duration(2000)
      .on("start", function() { // <-- Executes at start of transition
      d3.select(this).attr("height", function(d){ return 0;})
      ;})

      .attr("x", function(d) { return x(d.name); })
      .attr("width", x.bandwidth())
      .attr("y", function(d) { return y(d.mean); })
      .attr("height", function(d) { return height - y(d.mean); })
      .attr("fill", function(d){ return color(d.mean);})

      .on("end", function() { // <-- Executes at end of transition
      d3.select(this).transition();
      d3.select(this).append("title").text(function(d) {
        var numb =d.mean;
        var numbop =d.open;
        var result= d.open-d.mean;
        return "Valor: "+ numb.toFixed(2) +" +/- " + ""+ result.toFixed(2);});
      });


  svg.selectAll("circle")
      .data(data)
      .enter().append("circle")
      .attr("cx", function(d) { return x(d.name) + x.bandwidth()/2; })
      .attr("cy", function(d) { return y(d.mean); })
      .attr("r",4)
      .attr("fill","gray")
  ;

  for (var i = 0; i < data.length; i++) {
      svg.append("rect").attr("class","whiskertop");
      svg.append("rect").attr("class","whiskerbottom");
  }

  svg.selectAll(".whiskertop").data(data)
      .attr("x",function(d){return x(d.name) + x.bandwidth()/2.4})
      .attr("y", function(d) { return y(d.open); })
      .attr("width",10)
      .attr("height",2)
      .attr("fill","gray");

  svg.selectAll(".whiskerbottom").data(data)
      .attr("x",function(d){return x(d.name) + x.bandwidth()/2.4})
      .attr("y", function(d) { return y(d.close); })
      .attr("width",10)
      .attr("height",2)
      .attr("fill","gray")
      .append("title").text(function(d) {
        var numb =d.close
        return "Valor: "+ numb.toFixed(2);});

  svg.selectAll("line").data(data).enter().append("line").attr("x1", function(d) { return x(d.name) + x.bandwidth()/2; })
      .attr("y1", function(d){return y(d.open);})
      .attr("x2", function(d) { return x(d.name) + x.bandwidth()/2; })
      .attr("y2", function(d){return y(d.close);})
      .attr("stroke-width", 1)
      .attr("stroke", "gray")
      .style("opacity","0.5")
      .style("stroke-dasharray", ("3, 3"));
  // add the x Axis
    svg.append("g")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));
  // add the y Axis
    svg.append("g")
      .call(d3.axisLeft(y));

    d3.selectAll(".domain").attr("stroke","#EBE8E8");
    d3.selectAll(".tick").attr("stroke","beige").attr("stroke-width","0.4");
var imagen = svg.append("svg:image")
    .attr("transform","translate("+width+","+ height+") rotate(-90)")
      .attr('width', height)
    .attr('height', 10)

    .attr("xlink:href","legend.png");

    //});
}

////////////////Función principal////////////////////////////////////////
function principalBullet(data,container,title){
    var margin = {top: 5, right: 40, bottom: 20, left: 70},
    width = parseInt(d3.select(container).style("width")) - margin.left - margin.right;
    height = 25;
    var chart = d3.bullet()
        .width(width)
        .height(height);
      var svg = d3.select(container).selectAll("svg")
          .data(data)
        .enter().append("svg")
          .attr("class", "bullet")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")")        
          .call(chart);

      if(title) {
        var title = svg.append("g")
          .style("text-anchor", "end")
          .attr("transform", "translate(-6," + height / 2 + ")");

        title.append("text")
            .attr("class", "title")
            .text(function(d) { return d.seccion; });
      } 
}
////////////////////////////// Función de atributos y parámetros de configuración//////////////////////////
 d3.bullet = function() {
    var orient = "left", 
        reverse = false,
        duration = 1000,
        ranges = bulletRanges,
        markers = bulletMarkers,
        width = 380,
        height = 30;
  // For each small multiple…
  function bullet(g) {
    g.each(function(d, i) {
      var rangez = ranges.call(this, d, i).slice().sort(d3.descending),
        markerz = markers.call(this, d, i).slice().sort(d3.descending),
        g = d3.select(this);
      // Compute the new x-scale.
      var x1 = d3.scale.linear()
        .domain([0, Math.max(rangez[0], markerz[0], 6)])
        .range(reverse ? [width, 0] : [0, width]);
      // Derive width-scales from the x-scales.
      w1 = bulletWidth(x1);
      // Update the range rects.
      var range = g.selectAll("rect.range")
        .data(rangez);
      // Rect categories         
        range.enter().append("rect")
          .attr("class", function(d, i) { return "range s" + i; })
          .transition()
          .duration(duration)
          .attr("x", reverse ? x1 : 0)
          .attr("width", w1)
          .attr("height", height);
      // Update the marker lines.
      var marker = g.selectAll("line.marker")
        .data(markerz);
      
        marker.enter().append("circle")
          .attr("class", "marker")
          .attr("cx", x1(1.268*markerz-(1/2)))
          .attr("cy", height / 2.15)
          .attr("r",4.5);
      
        marker.on("mouseover",function(d){
        marker.attr("r", 10);});

        marker.on("mouseout", function(d){
        marker.attr("r",4.5);})
    });
  }
  // ranges (bad, satisfactory, good)
  bullet.ranges = function(x) {
    if (!arguments.length) return ranges;
    ranges = x;
    return bullet;
  };
  // markers (previous, goal)
  bullet.markers = function(x) {
    if (!arguments.length) return markers;
    markers = x;
    return bullet;
  };
  bullet.width = function(x) {
    if (!arguments.length) return width;
    width = x;
    return bullet;
  };
  bullet.height = function(x) {
    if (!arguments.length) return height;
      height = x;
      return bullet;
    };
    return bullet;
  };  

  function bulletRanges(d) {


    var count = d.valor;
    var array1 = [];

    for(var i=1;i<=count;i++)
    {
        var name = i;
        array1.push(name);
    }
    rango = array1; 
    return rango;
  }

  function bulletMarkers(d) {
    return d.contC;
  }

  function bulletWidth(x) {

    var x0 = x(0);
    return function(d) {
    return Math.abs(x(d)*1.3 - x0);
  };
}

