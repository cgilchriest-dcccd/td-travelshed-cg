'use strict';
const sass = require('node-sass');

module.exports = function (grunt) {

    require('time-grunt')(grunt);

    require('jit-grunt')(grunt,{
        useminPrepare:'grunt-usemin'
    });

    grunt.initConfig({
        sass: {
            options: {
                implementation: sass
            },
            dist: {
                files: {
                    'css/style-scss.css': 'css/style-scss.scss'
                }
            }
        },
        watch: {
            files: 'css/*scss',
            tasks: ['sass']
        },
        browserSync: {
            dev: {
                bsFiles: {
                    src: [
                        'css/*.css',
                        '*.html',
                        'js/*.js',
                    ]
                },
                options: {
                    watchTask: true,
                    server: {
                        baseDir: './'
                    }
                }
            }
        },
        copy: {
            html: {
                files: [{
                    expand: true,
                    dot: true,
                    cwd: './',
                    src: ['*.html'],
                    dest: 'dist'
                }]
            },
            fonts: {
                files: [{
                    expand: true,
                    dot: true,
                    cwd: 'node_module/font-awesome',
                    src: ['fonts/*.*'],
                    dest: 'dist'
                }]
            }
        },
        clean: {
            build: {
                src: ['dist/']
            }
        },
        imagemin: {
            dynamic: {
                files: [{
                    expand: true,
                    dot: true,
                    cwd: './',
                    src: ['img/*.{png,jpg,gif}'],
                    dest: 'dist'
                }]
            }
        },
        useminPrepare: {
            foo: {
                dest: 'dist',
                src: ['pop.html','res.html','work.html']
            },
            options: {
                flow: {
                    steps: {
                        css: ['cssmin'],
                        js: ['uglify']
                    },
                    post: {
                        css: [{
                            name: 'cssmin',
                            createConfig: function (context, block) {
                                var generated = context.options.generated;
                                generated.options = {
                                    keepSpecialComments: 0, rebase: false
                                };
                            }
                        }]
                    }
                }
            }
        },
        concat:{
            options:{
                separator:';'
            },
            dist:{}
        },
        uglify:{
            dist:{}
        },
        cssmin:{
            dist:{}
        },
        filerev:{
            options:{
                encoding:'utf8',
                algorithm:'md5',
                length:20
            },
            release:{
                files:[{
                    src:[
                        'dist/js/*.js',
                        'dist/css/*.css'
                    ]
                }]
            }
        },
        usemin:{
            html:['dist/pop.html','dist/res.html','dist/work.html'],
            options:{
                assetsDirs:['dist','dist/css','dist/js']
            }
        },
        htmlmin:{
            dist:{
                options:{
                    collapseWhiteSpace:true
                },
                files:{
                    'dist/pop.html':'dist/pop.html',
                    'dist/res.html':'dist/res.html',
                    'dist/work.html':'dist/work.html'
                }
            }
        }
    });

    grunt.registerTask('css', ['sass']);
    grunt.registerTask('default', ['browserSync', 'watch']);
    grunt.registerTask('build', [
        'clean',
        'copy',
        'imagemin',
        'useminPrepare',
        'concat',
        'cssmin',
        'uglify',
        'filerev',
        'usemin',
        'htmlmin'
    ]);
};