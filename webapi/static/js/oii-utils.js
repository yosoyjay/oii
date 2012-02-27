/* from http://stackoverflow.com/questions/2573521/how-do-i-output-an-iso-8601-formatted-string-in-javascript */
/* use a function for the exact format desired... */
function iso8601(d){
 function pad(n){return n<10 ? '0'+n : n}
 return d.getUTCFullYear()+'-'
      + pad(d.getUTCMonth()+1)+'-'
      + pad(d.getUTCDate())+'T'
      + pad(d.getUTCHours())+':'
      + pad(d.getUTCMinutes())+':'
      + pad(d.getUTCSeconds())+'Z'}

/* get URL query parameters. */
function query_param(name,default_value) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
    var regexS = "[\\?&]" + name + "=([^&#]*)";
    var regex = new RegExp(regexS);
    var results = regex.exec(window.location.href);
    if (results == null)
        return default_value;
    else
        return results[1];
}

/* make a JSON AJAX request */
function with_json_request(url, fn) {
    $.ajax({
        url : url,
        type : 'GET',
        dataType : 'json',
        success : fn
    });
}

/* console log */
function clog(s) {
    if (window.console) console.log(s);
}