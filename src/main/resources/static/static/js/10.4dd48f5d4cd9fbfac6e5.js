webpackJsonp([10],{Hmr8:function(e,t,n){"use strict";Object.defineProperty(t,"__esModule",{value:!0});n("0xDb");var a={data:function(){return{dataList:[],jsonData:"",search:{fileName:"",type:"",preRelationViewUrl:"",preAbsolutionViewUrl:"",createTime:""}}},mounted:function(){},created:function(){this.getResourceFileDetails()},watch:{},methods:{getResourceFileDetails:function(){var e=this;e.$http.post(e.api.getResourceFileDetails,e.search,{},function(t){e.dataList=t.content},function(t){e.$message({type:"error",message:"请求发生异常",duration:500})})},preByFileName:function(e){window.open(e,"_blank")},downloadByFileName:function(e){window.open(e,"_self")},deleteFileByName:function(e){var t=this;t.$http.get(t.api.deleteResourceFileByName,{params:{fileName:e}},function(e){0==e.code?(t.postForm=e.content,t.$message({type:"success",message:"删除成功",duration:2e3}),t.getResourceFileDetails()):t.$message({type:"error",message:e.msg,duration:2e3})},function(e){t.$message({type:"warning",message:"请求异常",duration:1e3})})},routerToAdd:function(){window.open("#/ResourceManagerAdd","_self")},routerToView:function(e){window.open("#/ResourceManagerView?fileName="+e,"_self")},copy:function(e){var t=document.createElement("input");t.value=e,document.body.appendChild(t),t.select(),document.execCommand("Copy"),document.body.removeChild(t),this.$message({type:"success",message:"复制成功",duration:1e3})}}},i={render:function(){var e=this,t=e.$createElement,n=e._self._c||t;return n("div",{staticClass:"app-container"},[n("div",{staticClass:"mt20"},[n("el-form",{attrs:{inline:!0,size:"mini"}},[n("el-form-item",[n("el-button",{staticClass:"el-button-search",attrs:{type:"primary"},on:{click:function(t){return e.routerToAdd()}}},[e._v("添加\n                ")])],1)],1)],1),e._v(" "),n("div",{staticClass:"app-list"},[n("div",{staticClass:"app-tab"},[n("table",[e._m(0),e._v(" "),e._m(1),e._v(" "),n("tbody",e._l(e.dataList,function(t,a){return n("tr",[n("td",[e._v(e._s(a+1))]),e._v(" "),n("td",[e._v(e._s(t.fileName))]),e._v(" "),n("td",[e._v(e._s(t.type))]),e._v(" "),n("td",[e._v(e._s(t.createTime))]),e._v(" "),n("td",[n("span",{on:{click:function(n){return e.deleteFileByName(t.fileName)}}},[e._v("删除")]),e._v(" "),n("span",{on:{click:function(n){return e.preByFileName(t.preRelationViewUrl)}}},[e._v("预览")]),e._v(" "),n("span",{on:{click:function(n){return e.downloadByFileName(t.downloadUrl)}}},[e._v("下载")]),e._v(" "),n("span",{on:{click:function(n){return e.routerToView(t.fileName)}}},[e._v("查看")]),e._v(" "),n("span",{on:{click:function(n){return e.copy(t.preRelationViewUrl)}}},[e._v("复制路径")])])])}),0)]),e._v(" "),n("p",{directives:[{name:"show",rawName:"v-show",value:0==e.total,expression:"total == 0"}],staticClass:"no-data-tip"},[e._v("没有找到相关数据！")]),e._v(" "),n("div",[n("pre",[e._v(e._s(e.jsonData))])])]),e._v(" "),n("div",{directives:[{name:"show",rawName:"v-show",value:e.total>0,expression:"total > 0"}],staticClass:"mt20"},[n("el-pagination",{attrs:{background:"","current-page":e.currentPage,"page-size":e.pageSize,layout:"total,prev, pager, next",total:e.total},on:{"current-change":e.handleCurrentChange,"update:currentPage":function(t){e.currentPage=t},"update:current-page":function(t){e.currentPage=t}}})],1)])])},staticRenderFns:[function(){var e=this,t=e.$createElement,n=e._self._c||t;return n("thead",[n("tr",[n("th",[e._v("id")]),e._v(" "),n("th",[e._v("fileName")]),e._v(" "),n("th",[e._v("type")]),e._v(" "),n("th",[e._v("createTime")]),e._v(" "),n("th",[e._v("操作")])])])},function(){var e=this,t=e.$createElement,n=e._self._c||t;return n("tr",[n("th",[e._v("序号")]),e._v(" "),n("th",[e._v("文件名称")]),e._v(" "),n("th",[e._v("文件类型")]),e._v(" "),n("th",[e._v("创建的时间")]),e._v(" "),n("th",[e._v("操作")])])}]};var r=n("VU/8")(a,i,!1,function(e){n("lSOV")},null,null);t.default=r.exports},lSOV:function(e,t){}});
//# sourceMappingURL=10.4dd48f5d4cd9fbfac6e5.js.map