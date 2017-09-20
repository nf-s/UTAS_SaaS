API_KEY = "hello";

Dropzone.autoDiscover = false;

window.onload = function () {
    menuItems=document.getElementsByClassName("pure-menu-link");

    for (var i =0;i<menuItems.length; i++) {
        menuItems[i].addEventListener("click", function () {
            for (var j =0;j<menuItems.length; j++) {
                menuItems[j].className="pure-menu-link";
            }
            this.className="pure-menu-link pure-menu-selected";
            this.blur();

            switch(this.href.split("#")[1]) {
                case "new-job": showNewJob(); break;
                case "inactive-jobs": showInactiveJobs(); break;
                case "active-jobs": showActiveJobs(); break;
            }
        });
    }

    initJobTable();
    showActiveJobs();
}

function showNewJob() {
    document.location.hash = 'new-job';
    showJobForm();
    initJobForm();
}

function showEditJob(jobId) {
    document.location.hash = 'edit-job';
    showJobForm();
    initJobForm(jobId);
}

function showJobForm() {
    document.getElementById('job-table').style.display="none";
    document.getElementById('job-form').style.display="block";
}

function showInactiveJobs() {
    document.location.hash = 'inactive-jobs';

    document.getElementById('job-table').style.display="block";
    document.getElementById('job-form').style.display="none";

    document.getElementById('inactive-job-table').style.display="block";
    document.getElementById('active-job-table').style.display="none";

    if (app != undefined) {
        app.$refs.vuetableInactiveJobTable.reload()
    }
}

function showActiveJobs() {
    document.location.hash = 'active-jobs';

    document.getElementById('job-table').style.display="block";
    document.getElementById('job-form').style.display="none";

    document.getElementById('active-job-table').style.display="block";
    document.getElementById('inactive-job-table').style.display="none";

    if (app != undefined) {
        app.$refs.vuetableActiveJobTable.reload()
    }
}

function initJobTable() {
    Vue.use(Vuetable);
    app = new Vue({
      el: '#app',
      components:{
       'vuetable-pagination': Vuetable.VuetablePagination
      },
      data: {
        activeFields: ['type', 'id','status','date-started','__slot:actions'],
        inactiveFields: ['type', 'id','status','date-started','__slot:actions'],
        activeApiUrl: "http://localhost:8081/test/job/active",
        inactiveApiUrl: "http://localhost:8081/test/job/inactive"
      },
      computed:{
          paginationInfo () {
            if (this.tablePagination == null || this.tablePagination.total == 0) {
                return this.$props.noDataTemplate
            }
        },
     },
     methods: {
        onPaginationData (paginationData) {
          this.$refs.pagination.setPaginationData(paginationData)
        },
        onChangePage (page) {
          this.$refs.vuetable.changePage(page)
        },
        editJobClick(rowData){
            //rowData['id']
            showEditJob(rowData['id']);
        },
        launchJobClick(rowData){
            launchJob(rowData['id']);
        },
        deleteJobClick(rowData){
            deleteJob(rowData['id']);
        },
        stopJobClick(rowData){
            stopJob(rowData['id']);
        }
    },
    props:{
        noDataTemplate: {
          type: String,
          default() {
            return 'No relevant data'
          }
        }
      },
    })
}

function launchJob(jobId) {
    axios.put('/test/job/'+jobId+'/launch')
    .then(function (response) {
        console.log(response);
        showInactiveJobs();
    })
    .catch(function (error) {
        console.log(error);
    });
}

function stopJob(jobId) {
    axios.put('/test/job/'+jobId+'/stop')
    .then(function (response) {
        console.log(response);
        showActiveJobs();
    })
    .catch(function (error) {
        console.log(error);
    });
}

function deleteJob(jobId) {
    axios.delete('/test/job/'+jobId)
    .then(function (response) {
        console.log(response);
    })
    .catch(function (error) {
        console.log(error);
    });
}

function initJobForm(jobId="") {
    // Get form-json-schema
    axios.get('/json/spark_form.json')
    .then(function (response) {
        jobFormSchema = response.data
        clearForm();

        // New job - get new job id
        if (jobId == "") {
            axios.post('/test/job/')
            .then(function (response) {
                console.log("received new job id");
                jobId = response.data['job-id'];
                initDropzone(jobId, jobFormSchema);
                loadForm(jobFormSchema, jobId);
            })
            .catch(function (error) {
                console.log(error);
            });
        } 
        // Edit job - get job form data
        else {
            axios.get('/test/job/'+jobId)
            .then(function (response) {
                console.log("received job data");      
                initDropzone(jobId, jobFormSchema);
                if (response.data['form_data'] != undefined) {
                    loadForm(jobFormSchema, jobId, response.data);
                } else {
                    loadForm(jobFormSchema, jobId);
                }
               
            })
            .catch(function (error) {
                console.log(error);
            });
        }

        
        console.log("received form json");
    })
    .catch(function (error) {
        console.log(error);
    });
}

function loadForm(jobFormSchema, jobId, data={}, useExistingFormData=false) {
    form_data={}

    if (data['form_data'] != undefined) {
        form_data=data['form_data']
    }

    if (typeof bf !== 'undefined') {
        // If no data has been set - use data from previous form instance
        if (data['form_data'] === undefined && useExistingFormData) {
            form_data = bf.getData();
        } 
        clearForm();
    }

    if (data['files_uploaded'] != undefined) {
        jobFormSchema.definitions.uploadedfiles.enum = remove_duplicates(
            jobFormSchema.definitions.uploadedfiles.enum.concat(
            data['files_uploaded']));
    }
    
    BrutusinForms = brutusin["json-forms"];

    BrutusinForms.addDecorator(function (element, formSchema) {
        if (element.tagName) {
            var tagName = element.tagName.toLowerCase();
            if (tagName === "form") {
                element.className += " pure-form";
            } else if (tagName === "tr") {
                element.className += " pure-control-group";
            } else if (tagName === "input") {
                if (element.type === "text") {
                    if (formSchema.formHeader != undefined) {
                        element.parentElement.className = "form_title_input";
                    }
                } else if (element.type === "file") {
                    /*var div = document.createElement("div");
                    div.id = "dropzone_"+element.id;
                    var parent = element.parentNode;
                    parent.insertBefore(div, element);
                    //div.appendChild(element);
                    parent.removeChild(element);
                    var myDropzone = new Dropzone(div, { url: "/file/post"});*/
                }
            } else if (tagName === "label") {
                if (formSchema.formHeader != undefined){
                    element.parentElement.className = "form_title_label";
                } else if (element.title != "") {
                    element.innerHTML += "<br/><span class='legend_description'>"+element.title+"</span>";
                    element.title = "";
                }
            } else if (tagName === "textarea") {
                if (formSchema.stylesheetClass != undefined) {
                    element.className += " "+formSchema.stylesheetClass;
                }
            } else if (tagName === "select") {
                
            }
        }
    });

    bf = BrutusinForms.create(jobFormSchema);
    container = document.getElementById('job-form-container');

    bf.render(container, form_data); 
    

    // Submit form button
    document.getElementById("job-form-submit-btn").addEventListener("click", function () {
        axios.put('/test/job/'+jobId, bf.getData())
        .then(function (response) {
            console.log(response);
            
            showInactiveJobs();
            clearForm();
            clearDropzone();
        })
        .catch(function (error) {
            console.log(error);
        });
        return true;
    });
    
}

function clearForm() {
    var container = document.getElementById('job-form-container');
    while (container.hasChildNodes()) {
        container.removeChild(container.lastChild);
    }
}

function initDropzone(jobId, jobFormSchema) {
    clearDropzone();
    // Create + setup Dropzone
    myDropzone = new Dropzone("div#job-form-upload", { 
        url: "/test/job/"+jobId+"/file",
        params: {apikey: API_KEY},
        dictDefaultMessage: "Drop files here to upload.<br/><br/>"+
                "<strong>Note:</strong><br/>"+
                "Folder structure is not preserved<br/>"+
                "Files with the same filename will overwrite existing files",
        createImageThumbnails: false,
        //uploadMultiple: true, 
        init: function() {
            this.on("queuecomplete", function() { 
                // Ask server to update form data with uploaded files
                axios.post('/test/job/'+jobId+'/files/form_data')
                .then(function (response) {
                    if (response.statusText == 'OK') {
                        console.log("received form data");
                        loadForm(jobFormSchema, jobId, response.data, true);
                    } else {
                        console.log("no form data received");
                    }
                })
                .catch(function (error) {
                    console.log(error);
                });
            });
        }
    });
}

function clearDropzone() {
    if (typeof myDropzone !== 'undefined') {
        myDropzone.destroy();
    }
}

function remove_duplicates(arr) {
    var obj = {};
    var ret_arr = [];
    for (var i = 0; i < arr.length; i++) {
        obj[arr[i]] = true;
    }
    for (var key in obj) {
        ret_arr.push(key);
    }
    return ret_arr;
}

// START MENU FUNCTIONS
(function (window, document) {
    
        var layout   = document.getElementById('layout'),
            menu     = document.getElementById('menu'),
            menuLink = document.getElementById('menuLink'),
            content  = document.getElementById('main');
    
        function toggleClass(element, className) {
            var classes = element.className.split(/\s+/),
                length = classes.length,
                i = 0;
    
            for(; i < length; i++) {
              if (classes[i] === className) {
                classes.splice(i, 1);
                break;
              }
            }
            // The className is not found
            if (length === classes.length) {
                classes.push(className);
            }
    
            element.className = classes.join(' ');
        }
    
        function toggleAll(e) {
            var active = 'active';
    
            e.preventDefault();
            toggleClass(layout, active);
            toggleClass(menu, active);
            toggleClass(menuLink, active);
        }
    
        menuLink.onclick = function (e) {
            toggleAll(e);
        };
    
        content.onclick = function(e) {
            if (menu.className.indexOf('active') !== -1) {
                toggleAll(e);
            }
        };
    
    }(this, this.document));
    