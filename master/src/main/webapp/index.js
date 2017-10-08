// -----------------------------------------------------------
// CONSTANTS
// -----------------------------------------------------------
API_KEY = "hello";
const MASTER_API_ROOTURL = "http://localhost:8081/api/client/";

// -----------------------------------------------------------
// VUE SETUP
// -----------------------------------------------------------
Vue.use(Vuetable);
/*Vue.component('modal', {
    template: '#modal-template'
})*/

app = new Vue({
    el: '#app',
    components:{
        'vuetable-pagination': Vuetable.VuetablePagination
    },
    data: {
        selectedJobId: null,

        activeApiUrl: MASTER_API_ROOTURL+"job/active",
        activeFields: ['type', 'id', 'description', 'status','date-created','running-time','__slot:actions'],

        inactiveApiUrl: MASTER_API_ROOTURL+"job/inactive",
        inactiveFields: ['type', 'id','description','status','date-created','__slot:actions'],
        
        showJobResultsModal: false,
        jobResultsApiUrl: "",
        jobResultsFeilds: ['filename', 'size','__slot:actions']
    },
    computed:{
        paginationInfo () {
            if (this.tablePagination == null || this.tablePagination.total == 0) {
                return this.$props.noDataTemplate
            }
        },
    },
    methods: {
        test(str) {
            alert(str);
        },
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
        relaunchJobClick(rowData){
            //launchJob(rowData['id']);
        },
        deleteJobClick(rowData){
            deleteJob(rowData['id']);
        },
        stopJobClick(rowData){
            stopJob(rowData['id']);
        },
        viewJobLogClick(rowData) {
            selectedJobId=rowData['id'];
        },
        viewJobResultsClick(rowData) {
            this.$data.selectedJobId=rowData['id'];
            this.$data.jobResultsApiUrl=MASTER_API_ROOTURL+"job/"+rowData['id']+"/results/filedetails";
            this.$refs.vuetableJobResultsTable.reload();
            this.$data.showJobResultsModal = true;
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

// -----------------------------------------------------------
// ON WINDOW LOAD
// -----------------------------------------------------------
window.onload = function () {
    document.getElementById('layout').style="display:block";

    setMenuItemClickListeners(document.getElementsByClassName("pure-menu-link"));
    setMenuItemClickListeners(document.getElementsByClassName("other-menu-buttons"));

    showActiveJobs();
}

// -----------------------------------------------------------
// NAVIGATION FUNCTIONS
// -----------------------------------------------------------
function setMenuItemClickListeners(menuItems) {
    for (var i =0;i<menuItems.length; i++) {
        menuItems[i].addEventListener("click", function () {         
            switch(this.href.split("#")[1]) {
                case "new-job": showNewJob(); break;
                case "inactive-jobs": showInactiveJobs(); break;
                case "active-jobs": showActiveJobs(); break;
            }
        });
    }
}
        
// Menu item ids:
// - "new-job"
// - "inactive-jobs"
// - "active-jobs"
// - "settings"
// - "help"
// - "logout"
function highlightMenuItem(selected) {
    menuItems=document.getElementsByClassName("pure-menu-link");
    for (var j =0;j<menuItems.length; j++) {
        if (menuItems[j].id == "menu-"+selected) {
            menuItems[j].className="pure-menu-link pure-menu-selected";
            menuItems[j].blur();
        } else {
            menuItems[j].className="pure-menu-link";
        }
    }
}

// Side Menu navigation - burger menu icon
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

// -----------------------------------------------------------
// APP MAIN FUNCTIONS:
//
// - Create new job
// - Edit job
// - Show active jobs
// - Show inactive jobs
// -----------------------------------------------------------
function showNewJob() {
    highlightMenuItem('new-job');
    document.location.hash = 'new-job';
    showJobForm();
    initJobForm();
}

function showEditJob(jobId) {
    highlightMenuItem('new-job');
    document.location.hash = 'edit-job';
    showJobForm();
    initJobForm(jobId);
}

function showJobForm() {
    document.getElementById('job-table').style.display="none";
    document.getElementById('job-form').style.display="block";
}

function showInactiveJobs() {
    highlightMenuItem('inactive-jobs');
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
    highlightMenuItem('active-jobs');
    document.location.hash = 'active-jobs';
    
    document.getElementById('job-table').style.display="block";
    document.getElementById('job-form').style.display="none";
    
    document.getElementById('active-job-table').style.display="block";
    document.getElementById('inactive-job-table').style.display="none";
    
    if (app != undefined) {
        app.$refs.vuetableActiveJobTable.reload()
    }
}


// -----------------------------------------------------------
// JOB FUNCTIONS
// -----------------------------------------------------------
function launchJob(jobId) {
    axios.put(MASTER_API_ROOTURL+'job/'+jobId+'/launch')
    .then(function (response) {
        console.log(response);
        showActiveJobs();
    })
    .catch(function (error) {
        console.log(error);
    });
}

function stopJob(jobId) {
    axios.put(MASTER_API_ROOTURL+'job/'+jobId+'/stop')
    .then(function (response) {
        console.log(response);
        showActiveJobs();
    })
    .catch(function (error) {
        console.log(error);
    });
}

function deleteJob(jobId) {
    axios.delete(MASTER_API_ROOTURL+'job/'+jobId)
    .then(function (response) {
        console.log(response);
        showInactiveJobs();
    })
    .catch(function (error) {
        console.log(error);
    });
}

// -----------------------------------------------------------
// NEW/EDIT JOB FORM FUNCTIONS
// -----------------------------------------------------------
function initJobForm(jobId="") {
    // Get form-json-schema
    axios.get('/json/spark_form.json')
    .then(function (response) {
        jobFormSchema = response.data
        clearForm();
        
        // New job - get new job id
        if (jobId == "") {
            axios.post(MASTER_API_ROOTURL+'job/')
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
            axios.get(MASTER_API_ROOTURL+'job/'+jobId)
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
        axios.put(MASTER_API_ROOTURL+'job/'+jobId, bf.getData())
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

    // Discard form button
    document.getElementById("job-form-discard-btn").addEventListener("click", function () {
        deleteJob(jobId)
        showInactiveJobs();
        clearForm();
        clearDropzone();
        return true;
    });
            
}

function clearForm() {
    var container = document.getElementById('job-form-container');
    while (container.hasChildNodes()) {
        container.removeChild(container.lastChild);
    }
}

// -----------------------------------------------------------
// DROPZONE FUNCTIONS
// -----------------------------------------------------------
Dropzone.autoDiscover = false;
function initDropzone(jobId, jobFormSchema) {
    clearDropzone();
    // Create + setup Dropzone
    myDropzone = new Dropzone("div#job-form-upload", { 
        url: MASTER_API_ROOTURL+"job/"+jobId+"/resources/file",
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
                axios.post(MASTER_API_ROOTURL+'job/'+jobId+'/resources/process_uploaded')
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

// Clears dropzone
function clearDropzone() {
    if (typeof myDropzone !== 'undefined') {
        myDropzone.destroy();
    }
}

// -----------------------------------------------------------
// UTILITY FUNCTIONS
// -----------------------------------------------------------

// Removed duplicates from array
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

