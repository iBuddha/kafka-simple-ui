(function () {
    var fixDataMapIndexes = function() {
        $('[id*=topics]').each(function(i, element){
            if (i % 6 == 1) {
                var itemNumber = Math.floor(i / 6);
                element.id = 'topics_' + itemNumber + '_name';
                element.name = 'topics[' + itemNumber + '].name';
            }

            if (i % 6 == 3) {
                var itemNumber = Math.floor(i / 6);
                element.id = 'topics_' + itemNumber + '_partitions';
                element.name = 'topics[' + itemNumber + '].partitions';
            }

            if (i % 6 == 5) {
                var itemNumber = Math.floor(i / 6);
                element.id = 'topics_' + itemNumber + '_replicas';
                element.name = 'topics[' + itemNumber + '].replicas';
            }
        })
    };

    $('.job-data-map').on('click', '.job-data-delete a', function () {
        $(this).parent().next().next().next().remove();
        $(this).parent().next().next().remove();
        $(this).parent().next().remove();
        $(this).parent().remove();

        fixDataMapIndexes();
    });

    $('.job-data-add a').click(function () {
        console.log('hello')
        var topicName = $(this).parent().prev().prev().prev().clone();
        var partitions = $(this).parent().prev().prev().clone();
        var replicas = $(this).parent().prev().clone();
        $(this).parent().prev().prev().prev().before($('<div class="job-data-delete text-right"><a href="#">delete</a></div>'));
        console.log(topicName)
        console.log(partitions)
        console.log(replicas)

        topicName.find('input').val('');
        partitions.find('input').val('');
        replicas.find('input').val('');

        $(this).parent().before(topicName)
        $(this).parent().before(partitions);
        $(this).parent().before(replicas);

        fixDataMapIndexes();
    });

    $("form").submit(function () {
        fixDataMapIndexes();
    });
})();